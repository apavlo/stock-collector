#!/usr/local/bin/perl

BEGIN {
   my $arch = `uname -m`; chomp($arch);
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/share/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl5");
   push(@INC, "/home/pavlo/Documents/Stocks/collector/modules");
}

use strict;
use warnings;
use POSIX;
use DBI;
use List::Util;
use Getopt::Long;
use IO::Handle;
use FileHandle;
use File::Basename;

use CollectorCommon qw(debug_print);

##
## Quahog recovery?
##
my $RECOVER_ARGS_FILE = "/tmp/" . basename($0) . ".args";
if ($ARGV[0] eq "=recover") {
   my $idx = 0;
   for my $arg (`cat $RECOVER_ARGS_FILE`) {
      chomp($arg);
      $ARGV[$idx++] = $arg;
   } # FOR
##
## Write out arguments our to a file
## We won't die, we'll just keep going
##
} elsif (open(ARGS_FILE, ">$RECOVER_ARGS_FILE")) {
   print ARGS_FILE join("\n", @ARGV);
   close(ARGS_FILE);
}

GetOptions("dbhost=s"    => \$main::opt_dbhost,
           "limit=s"     => \$main::opt_limit,
           "offset=s"    => \$main::opt_offset,
           "stdout=s"    => \$main::opt_stdout,
           "stderr=s"    => \$main::opt_stderr,
           "nosleep"     => \$main::opt_nosleep,
           "debug"       => \$main::opt_debug,
           "lock"        => \$main::opt_lock,
);

##
## Check the lock before we open up pipes to STDOUT and STDIN
## That way our monitoring script doesn't think that a job is
## executing when maybe it isn't really...
##
my $lock_id = (defined($main::opt_offset) ? $main::opt_offset : 0);
if (defined($main::opt_lock) && $main::opt_lock) {
   unless (CollectorCommon::lock($lock_id, "stocks", 120)) {
      die(debug_print("Failed to acquire lock for Lock ID \#$lock_id. Exiting..."));
   } # UNLESS
   CollectorCommon::unlock($lock_id, "stocks")
}

if (defined($main::opt_stdout)) {
   open (OUTPUT, ">$main::opt_stdout") || die $!;
   STDOUT->fdopen(\*OUTPUT, "w") || die $!;
}
if (defined($main::opt_stderr)) {
   open (ERROR, ">$main::opt_stderr")   || die $!;
   STDERR->fdopen(\*ERROR, "w")  || die $!;
}
STDOUT->autoflush;
STDERR->autoflush;

##
## Where we are executing
##
my $HOSTNAME = `hostname`;
chomp($HOSTNAME);
print debug_print("Started on '$HOSTNAME'");

##
## The failover directory is where we will store our data if we were not
## able to store it in the database. This makes sure that we keep records
## no matter what
##
my $FAILOVER_DIR = $CollectorCommon::BASE_DIR."/failover";

##
## Max clusters 
##
my $MAX_CLUSTERS = 10;
my $CLUSTER_RAND_FACTOR = 10;

##
## The URL we can lookup data from Yahoo
## Information about format parameters can be found here:
## -> http://www.gummy-stuff.org/Yahoo-data.htm
##
## Format: Symbol
##         Price
##         Last Trade Date
##         Last Trade Time
##         Volume
##         Ask Price
##         Bid Price
##         Ask Size
##         Bid Size
##
my $DATA_URL     = "http://download.finance.yahoo.com/d/quotes.csv?s=%s&f=sl1d1t1vb2b3a5b6&e=.csv";

$main::opt_dbhost = "localhost" unless (defined($main::opt_dbhost));
print debug_print("Connecting to database at '$main::opt_dbhost'") if ($main::opt_debug);
my $db = CollectorCommon::db_connect($main::opt_dbhost);
my %stocks = ( );

my $sql = "SELECT penny_stocks.* ".
          "  FROM penny_stocks ".
          ($#ARGV >= 0 ? " WHERE symbol IN (".join(",", map { $db->quote($_) } @ARGV).")" : "").
          " ORDER BY symbol ASC ";
if (defined($main::opt_limit)) {
   $sql .= "LIMIT $main::opt_limit ";
   $sql .= "OFFSET $main::opt_offset " if (defined($main::opt_offset) && $main::opt_offset > 0);
}
my $handle = $db->prepare($sql);
$handle->execute() || die $DBI::errstr;
print debug_print($sql) if ($main::opt_debug);
while (my $row = $handle->fetchrow_hashref()) {
   $stocks{$row->{'full_symbol'}} = $row->{'id'};
} # WHILE
$handle->finish();

##
## If we didn't get any stocks, we'll just quit
##
CollectorCommon::shutdown("No stocks were retrieved! Nothing to do!") unless (scalar keys(%stocks) > 0);
print debug_print("Starting quote collection for ".keys(%stocks)." stocks");

##
## We're going to keep looping and trying to get new data
##
my $flags;
my $ctr = 0;
my $stock_ctr = 0;
my $inserts = 0;

##
## This list is of the stocks where the last update was more than a day ago
## We will pull these from our main loop list, but then only check every 5 minutes
## 
my @slow_stocks = ( );

my $inserted = undef;
while (1) {
   my $output = "";
   $stock_ctr = 0;
   $inserts = 0;

   ##
   ## LOCK
   ##
   if (defined($main::opt_lock) && $main::opt_lock) {
      unless (CollectorCommon::lock($lock_id, "stocks", 120)) {
         print STDERR debug_print("Failed to acquire lock for Lock ID \#$lock_id. Exiting...");
         last;
      } # UNLESS
   }
   
   ##
   ## How far back should we check to see whether this is a slow stock
   ##
   my $last_updated = ((localtime(time))[2] > 10 ? 7200 : 57600);
   ##
   ## We need to know what minute we started in. This will help us
   ## determine how long we can sleep for once we process all our clusters
   ##
   my $start_min = (localtime(time))[1];

   ##
   ## Rather than worrying about whether the database connection will get dropped,
   ## we are just going to reconnect everytime we want to grab new data
   ##
   $db = CollectorCommon::db_connect($main::opt_dbhost);

   ##
   ## To avoid making all of our queries look the same, we're going 
   ## change the ordering of the symbols and cluster differently each time
   ## Although it is probably me being paranoid, we are also going to
   ## randomize the number stocks requested each time
   ##
   my @stocks_queue = keys(%stocks);
   unless ($start_min % 5 == 0) {
      ##
      ## Remove the slow stocks
      ##
      my @temp = ();
      foreach my $stock (@stocks_queue) {
         push(@temp, $stock) unless (grep(/$stock/, @slow_stocks));
      }
      @stocks_queue = @temp;
   }
   @stocks_queue = List::Util::shuffle(@stocks_queue);
   
   my $clusters = int(rand($MAX_CLUSTERS) + 2);
   my $time_slot = int(60 / $clusters);
   for (my $idx = 0; $idx < $clusters; $idx++) {
      next unless ($#stocks_queue >= 0);
      ##
      ## Get the number of stocks we want to use for this cluster
      ## If there are no more stocks left, then we can just skip ahead
      ## If this is the last cluster, we need to get all the info
      ## Or if there are less than 10 seconds left in this minute block
      ##
      my @cur_stocks_queue = ( );
      if ($#ARGV >= 0 || ($idx + 1) == $clusters || ((localtime(time))[0] > 50)) {
         #print debug_print("\$idx = $idx   :: \$clusters = $clusters");
         @cur_stocks_queue = @stocks_queue;
         @stocks_queue = ( );
      } else {
         my $temp1 = int(rand($CLUSTER_RAND_FACTOR * 2));
         my $temp2 = int(keys(%stocks) / $clusters);
         my $temp3 = $CLUSTER_RAND_FACTOR;
         my $cnt = $temp1 + $temp2 - $temp3;
         my $temp4 = int(keys(%stocks));
#         my $cnt = ceil(int(rand($CLUSTER_RAND_FACTOR * 2)) + int(keys(%stocks) / $clusters) - $CLUSTER_RAND_FACTOR);
         while ($cnt-- > 0 && $#stocks_queue >= 0) {
            my $stock = pop(@stocks_queue);
            last unless (defined($stock));
            push(@cur_stocks_queue, $stock);
         } # WHILE
      }
      next unless ($#cur_stocks_queue >= 0);

      ##
      ## Execute our query to Yahoo!
      ## Divide them in half if they are over 200
      ##
      my $temp = 0;
      while ($#cur_stocks_queue >= 0) {
         my %stock_data = ( );
         my @sqls = ( );
         
         $temp++;
         my $length = ($#cur_stocks_queue > 199 ? 199 : $#cur_stocks_queue);
         my @url_stocks = splice(@cur_stocks_queue, 0, $length + 1);
         #print debug_print("[$idx] QUEUE: ".join(", ", @url_stocks));
         
         my $data = CollectorCommon::wget_get_url(sprintf($DATA_URL, join(",", @url_stocks)));
         my $cur_time = time();
         
         foreach my $line (split(/\n/, $data)) {
            print debug_print($line) if ($main::opt_debug);
            $stock_ctr++;
            $line =~ s/\r//g;
            #$line =~ s/N\\A//g;
            my @attrs = split(/,/, $line);
            ##
            ## Important!
            ## The Ask & Bid Sizes are messed up and sometimes have commas 
            ## in them!!! This sucks big time, so we need to check whether we need
            ## try to make sense of the data if there are more than the expected number
            ## of attributes
            ##
            ($attrs[7], $attrs[8]) = fix_attrs(@attrs) if ($#attrs > 8);
            ##
            ## Convert 'N/A' to 0
            ##
            for (my $attr_ctr = 4; $attr_ctr <= 9; $attr_ctr++) {
               $attrs[$attr_ctr] = 0.00 if (defined($attrs[$attr_ctr]) && $attrs[$attr_ctr] eq "N/A");
            } # FOR
            
            my $symbol = $attrs[0];
            $symbol =~ s/\"//g;

            my ($time, $timestamp) = convert_time($attrs[2], $attrs[3]);
            ##
            ## Manage the slow stock list
            ## If the stock is updated in the last 2 hours, we'll want to remove it from the list
            ## This helps us deal with the weekend problem
            ##
            if (defined($timestamp) && defined($inserted)) {
               if (($timestamp + $last_updated) < $cur_time) {
                  ##
                  ## Add
                  ##
                  unless (grep(/$symbol/, @slow_stocks)) {
                     push(@slow_stocks, $symbol);
                     $output .= debug_print("Added '$symbol' to slow stocks!");
                  } # UNLESS
               } elsif (grep(/$symbol/, @slow_stocks)) {
                  ##
                  ## Remove
                  ##
                  for (my $ctr = 0; $ctr <= $#slow_stocks; $ctr++) {
                     if ($slow_stocks[$ctr] eq $symbol) {
                        splice(@slow_stocks, $ctr, 1);
                        $output .= debug_print("Removed '$symbol' from slow stocks!");
                        last;
                     }
                  } # FOR
               }
            } elsif (defined($inserted) && !grep(/$symbol/, @slow_stocks)) {
               push(@slow_stocks, $symbol);
            }
            ##
            ## It would be great it if we could used batched INSERT commands
            ## But because it is quite likely that one of the queries will fail,
            ## we need to separate them out and execute them one-by-one
            ##
            die("ERROR: Undefined stock $symbol!!!!\n") unless (defined($stocks{$symbol}));
            my $sql = "INSERT INTO quotes VALUES ( ".
                     "  ".$db->quote($stocks{$symbol}).", ".                    # stock_id
                     "  ".$db->quote($attrs[1]).", ".                           # price
                     "  ".$db->quote(int($attrs[4])).", ".                      # volume
                     (defined($attrs[5]) ? $db->quote($attrs[5]) : "NULL").",". # ask
                     (defined($attrs[7]) ? $db->quote($attrs[7]) : "NULL").",". # ask size
                     (defined($attrs[6]) ? $db->quote($attrs[6]) : "NULL").",". # bid
                     (defined($attrs[8]) ? $db->quote($attrs[8]) : "NULL").",". # bid size
                     "  ".$db->quote($time).                                    # time
                     ")";
            push(@sqls, $sql) if (defined($time));
         } # FOREACH
         ##
         ## We need to know execute all our commands
         ## If any one fails, we'll dump out our data information in the failover directory
         ##
         my $failed;
         foreach (@sqls) {
            unless ($db->do($_)) {
               ##
               ## It's not really an error if it's a duplicate entry
               ##
               unless ($DBI::err == $CollectorCommon::DBI_DUPLICATE_ERROR_CODE) { 
                  print STDERR debug_print($DBI::errstr);
                  print STDERR debug_print($_);
                  write_failover($data) unless ($failed);
                  $failed = 1;
               } # UNLESS
            } else {
               my $sql_dump = $_;
               $sql_dump =~ s/[\s]+/ /g;
               $output .= debug_print($sql_dump);
               $inserts++;
               $inserted = 1;
            } # UNLESS
         } # FOREACH
      } # WHILE

      ##
      ## We need to sleep until the next time slot for a cluster
      ##
      my $sleep = (($idx + 1) * $time_slot) - (localtime(time))[0];
      sleep($sleep) if ($sleep > 0 && $#stocks_queue >= 0 && ($idx + 1) < $clusters);
   } # FOR
   ##
   ## We now want to sleep the rest of the time until the next minute
   ## Unless it is after 4:45 pm, then we're done for the day
   ## If all our processing took longer than a minute, then we need to cut
   ## down on the number of clusters and our randomizing factor
   ## This should help normalize the randomness distribution so that
   ## we don't have one cluster with a lot of processing but 
   ## another with just a little and a longer sleep time
   ##
   print $output unless($main::opt_debug);
   print debug_print("Number of Stocks Checked this Round: $stock_ctr");
   print debug_print("Number of New Quotes Inserted: $inserts");
   my ($sec, $min, $hour) = (localtime(time))[0,1,2];

   ##
   ## Unlock
   ##
   CollectorCommon::unlock($lock_id, "stocks") if (defined($main::opt_lock));
   
   if (defined($main::opt_nosleep) || $hour > 16 || ($hour == 16 && $min >= 25)) {
      #my $sleep = 46800;
      print debug_print("Done for the day!");
      #sleep($sleep);
      CollectorCommon::shutdown();
   } elsif ($start_min != $min) {
      ##
      ## It is possible that NTP or somebody changed the time on us, but
      ## it is probably not likely in the CS department
      ##
      $MAX_CLUSTERS-- if ($MAX_CLUSTERS > 1);
      $CLUSTER_RAND_FACTOR-- if ($CLUSTER_RAND_FACTOR > 1);
   ##
   ## Otherwise just sleep for all the time that we can get
   ##
   } else {
      my $sleep = 60 - $sec;
      if ($sleep > 0) {
         print debug_print("SLEEP: $sleep");
         sleep($sleep);
      }
   }
   $db->disconnect();
} # WHILE

## ------------------------------------------------------
## convert_time
## ------------------------------------------------------
sub convert_time {
   my ($date, $time) = @_;
   $date =~ s/\"//g;
   $time =~ s/\"//g;
   
   my ($month, $day, $year, $hour, $min, $am_pm);
   if ($date =~ m/([\d]{1,2})\/([\d]{1,2})\/([\d]{4,4})/) {
      ($month, $day, $year) = ($1, $2, $3);
   } else {
      return (undef);
   }
   if ($time =~ m/([\d]{1,2})\:([\d]{2,2})([ap]m)/) {
      ($hour, $min, $am_pm) = ($1, $2, $3);
      $hour += 12 if ($am_pm eq "pm" && $hour != 12);
   } else {
      return (undef);
   }
   return ("$year-$month-$day $hour:$min",
           mktime(0, $min, $hour, $day, $month - 1, $year - 1900));
}

## ------------------------------------------------------
## write_failover
## ------------------------------------------------------
sub write_failover {
   my ($data) = @_;
   my $dir = "$FAILOVER_DIR/$HOSTNAME";
   my $file = "$dir/data.".time().".$$.dump";

   ##
   ## Make sure our fail over dir is there
   ##
   system("mkdir -p $dir") unless (-d $dir);

   ##
   ## Write out to our file
   ##
   if (open(FILE, ">$file")) {
      print debug_print("FAILOVER: $file");
      print FILE $data;
      close(FILE);
   ##
   ## If we can't write to our file here, this is a BIG problem!
   ## So we should die and maybe send a notice email?? 
   ##
   } else {
      die("ERROR: FAILED TO WRITE FAILOVER FILE '$file'!!!\n");
   }
   return;
}

## ------------------------------------------------------
## fix_attrs
## ------------------------------------------------------   
sub fix_attrs {
   my (@attrs) = @_;
   my ($ret1, $ret2);

   ##
   ## If there are 10 attributes, then all we need to do is just put the
   ## attributes together
   ##
   if ($#attrs == 10) {
      $ret1 = int($attrs[7].$attrs[8]);
      $ret2 = int($attrs[9].$attrs[10]);
   ##
   ## Otherwise our logic is a bit more tricky
   ## Assume that the first number in the broken sequence that we 
   ## are trying to find is smaller than the second part
   } else {
      if (length($attrs[7]) < length($attrs[8])) {
         $ret1 = int($attrs[7].$attrs[8]);
         $ret2 = $attrs[9];
      } else {
         $ret1 = $attrs[7];
         $ret2 = int($attrs[8].$attrs[9]);
      }
   }
   return ($ret1, $ret2);
}
