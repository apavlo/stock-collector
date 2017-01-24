package CollectorCommon;

use strict;
use warnings;

use POSIX;
use DBI;
use List::Util;
use File::Basename;
use Cwd qw(realpath);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(wget_get_url elinks_get_url debug_print db_connect clean convert_date);

##
## Paths
##
our $BASE_DIR       = "/home/pavlo/Documents/Stocks";
my $LOCKS_DIR       = $BASE_DIR . "/locks";
my $WGET_LOCATION   = "/usr/bin/wget";
my $ELINKS_LOCATION = "/home/pavlo/Programs/elinks/bin/elinks";
my $COOKIE_FILE     = "/home/pavlo/.elinks/cookies";
my $USER_AGENTS     = $BASE_DIR . "/useragents.data";
my $WGET_FLAGS      = "-q --user-agent=\"%s\"";

my $DB_USER         = "stocks_user";
my $DB_PASS         = "***CHANGEME***";
my $SEND_EMAIL_TO   = "your\@email.com";
my $SEND_EMAIL_FROM = "StockCollector <$SEND_EMAIL_TO>";

our $DBI_DUPLICATE_ERROR_CODE = 1062;

our $NO_URLS_FOUND = "XXXXX";

##
## Email Queue
##
my @MESSAGE_QUEUE   = ( );

my $HOSTNAME = `hostname`;
chomp($HOSTNAME);

$SIG{INT} = \&shutdown;

my %MONTHS = ( "January"   => 1,
               "February"  => 2,
               "March"     => 3,
               "April"     => 4,
               "May"       => 5,
               "June"      => 6,
               "July"      => 7,
               "August"    => 8,
               "September" => 9,
               "October"   => 10,
               "November"  => 11,
               "December"  => 12
);

##
## Message URL Statuses
##
our $MSG_STATUS_OK        = 0;
our $MSG_STATUS_ERROR     = 1;
our $MSG_STATUS_NOSYMBOL  = 2;
our $MSG_STATUS_OLD       = 3;
our $MSG_STATUS_NOTPENNY  = 4;
our $MSG_STATUS_URLCHANGE = 5;
our $MSG_STATUS_SYMCHANGE = 6;
our $MSG_STATUS_INVALID   = 7;

##
## Parent Logic Ids
## This will allow us to figure out what the parent is
##
our $PARENT_NONE  = 0;  # No parent DEFAULT
our $PARENT_FIRST = 1;  # The parent is the first message in the list
our $PARENT_URL   = 2;  # Get the parent id from the url in 'parent_url'

## ------------------------------------------------------
## hostname
## ------------------------------------------------------
sub hostname {
   return ($HOSTNAME);
}

## ------------------------------------------------------
## lock
## ------------------------------------------------------
my %LOCKS = ( );
sub lock {
   my ($id, $type, $expire) = @_;
   my $lock = $LOCKS_DIR . "/lock.".(defined($type) ? "$type." : "").sprintf("%02d", $id);
   my $cur_time = time();
   $expire = 7200 unless (defined($expire));
   if (-f $lock) {
      ##
      ## Check to see the last touch time
      ##
      my $timestamp = int(`cat $lock`);
      die(debug_print("Failed to open lock file '$lock' on $HOSTNAME. Lock not old enough!")) unless ($timestamp + $expire < $cur_time);
   }
   open(FILE, ">$lock") || die(debug_print("Failed to open lock file '$lock' on $HOSTNAME"));
   print FILE $cur_time;
   close(FILE);
   $LOCKS{$id} = 1;
   return (1);
}
## ------------------------------------------------------
## unlock
## ------------------------------------------------------
sub unlock { 
   my ($id, $type) = @_;
   my $lock = $LOCKS_DIR . "/lock.".(defined($type) ? "$type." : "").sprintf("%02d", $id);
   if (defined($LOCKS{$id})) {
      unlink($lock) || die(debug_print("Failed to delete lock file '$lock' on $HOSTNAME"));
      delete($LOCKS{$id});
   }
   return (1);
}
## ------------------------------------------------------
## refresh
## ------------------------------------------------------
sub refresh {
   my ($id) = @_;
   my $lock = $LOCKS_DIR . "/lock.".sprintf("%02d", $id);
   open(FILE, ">$lock") || die(debug_print("Failed to open lock file '$lock' for refreshing on $HOSTNAME!"));
   print FILE time();
   close(FILE);
   return (1);
}

## ------------------------------------------------------
## wget_get_url
## ------------------------------------------------------
my $wget_ctr = 0;
my $wget_flags;
sub wget_get_url {
   my ($url) = @_;
   
   if (!defined($wget_flags) || ($wget_ctr++ % 103) == 0) {
      $wget_flags = sprintf($WGET_FLAGS, randomUserAgent());
   }
   my $cmd = "$WGET_LOCATION $wget_flags '$url' -O -";
   #print "$cmd\n";
   return (`$cmd`);
}

## ------------------------------------------------------
## elinks_get_url
## ------------------------------------------------------
sub elinks_get_url {
   my ($url) = @_;
   my $cmd = "${ELINKS_LOCATION} -source '$url'";
   return (`$cmd`);
}

## ------------------------------------------------------
## randomUserAgent
## ------------------------------------------------------
sub randomUserAgent {
   my $ret = "Mozilla/5.0 (X11; U; FreeBSD i386; en-US; rv:1.7.8) Gecko/20050609 Firefox/1.0.4";
   if (open(FILE, "<${USER_AGENTS}")) {
      my @lines = <FILE>;
      close(FILE);
      $ret = $lines[int(rand(@lines))];
   }
   chomp($ret);
   return ($ret);
}

## ------------------------------------------------------
## db_connect
## ------------------------------------------------------
my $db;
sub db_connect {
   my ($host) = @_;
   if (!defined($db) || !$db->ping) {
      my $DB_CONNECT_STR = "DBI:mysql:database=stocks;host=$host;";
      $db = DBI->connect_cached($DB_CONNECT_STR, $DB_USER, $DB_PASS, { PrintError => 0 });
      unless (defined($db)) {
         my $subject = "ERROR: Unable to connect to database on $host";
         my $message = "We were unable to connect to the MySQL database running on $host\n$DBI::errstr\n";
         send_email($subject, $message, 1);
         die(debug_print("$subject :: $DBI::errstr"));
      } # UNLESSs
   }
   return ($db);
}

## -----------------------------------------------------------
## debug_print
## -----------------------------------------------------------
sub debug_print {
   my ($str) = @_;
   eval {
      $str =~ s/[\s]{2,}/ /g;
   };
   print "ERROR: debug_print failed ($@), continuing...\n" if ($@);
   return (strftime('%m-%d-%Y %H:%M:%S - ', localtime)."$str\n");
}

## -----------------------------------------------------------
## debug_dump
## -----------------------------------------------------------
sub debug_dump {
   my (%data) = @_;
   foreach (keys %data) {
      print "$_:\t$data{$_}\n";
   }
   print "\n";
}

## -----------------------------------------------------------
## file_dump
## -----------------------------------------------------------
sub file_dump {
   my ($data, $file) = @_;
   if (defined($file) && length($file) > 0) {
      open(FILE, ">$file") || die(debug_print("Failed to open '$file' for writing!\nDumping output:\n$data"));
      print FILE $data;
      close(FILE);
   } else {
      print $data;
   }
   return (1);
}

## -----------------------------------------------------------
## shutdown
## -----------------------------------------------------------
sub shutdown {
   my ($message) = @_;
   $db->disconnect() if (defined($db));
   ##
   ## Remove all locks
   ##
   foreach my $id (keys %LOCKS) {
      unlock($id);
   }
   ##
   ## Send out any queued messages
   ##
   if ($#MESSAGE_QUEUE >= 0) {
      my $subject = basename($0)." Messages: ".($#MESSAGE_QUEUE + 1);
      my $message = "The collector program '$0' produced the following messages:\n";
      for (my $ctr = 0; $ctr <= $#MESSAGE_QUEUE; $ctr++) {
         $message .= sprintf("\n[%02d] %s\n%s\n", $ctr, $MESSAGE_QUEUE[$ctr]{"subject"}, $MESSAGE_QUEUE[$ctr]{"message"});
         chomp($message)
      } # FOR
      send_email($subject, $message, 1);
   }
   print debug_print("Shutting down!");
   ##
   ## If we have a message, then it's a die. Otherwise we exit nicely
   ##
   die(debug_print($message)) if (defined($message) && $message ne "INT");
   exit;
}

## -----------------------------------------------------------
## strip_html
## -----------------------------------------------------------
sub strip_html {
   my ($html) = @_;
   $html =~ s/<(?:[^>'"]*|(['"]).*?\1)*>//gs;
   return ($html);
} 

## -----------------------------------------------------------
## clean
## -----------------------------------------------------------
sub clean {
   my ($str) = @_;
   eval {
      ## HTML fixes
      $str =~ s/&amp;/\&/g;
      $str =~ s/&gt;/\>/g;
      $str =~ s/&lt;/\</g;
      $str =~ s/&nbsp;/ /g;
      $str =~ s/&quot;/\"/g;
      $str =~ s/&\#([\d]+);/chr($1)/eg;
      
      $str =~ s/<BR[\s]*[\/]?>/\n/gis;
      $str =~ s/<P>//gi;
      $str =~ s/[\r\n]+/\n/g;
      $str =~ s/^\n[\s]*//g;
      $str =~ s/\n[\s]*$//g;
      
      ## Remove non-ascii
      $str =~ s/[^[:ascii:]]//g;
   
      while ($str =~ s/(\n|\0|<br>|\s)$//i) { } 
      chomp($str);
   };
   print "ERROR: Clean failed ($@), continuing...\n" if ($@);
   return ($str);
}

## ------------------------------------------------------
## convert_date
## ------------------------------------------------------
sub convert_date {
   my ($sec, $min, $hour, $ampm, $day, $month, $year) = @_;
   ##
   ## Fix hour
   ##
   if (defined($ampm)) {
      $hour += 12 if ($ampm =~ /pm/i && $hour != 12); 
      $hour -= 12 if ($ampm =~ /am/i && $hour == 12);
   }
   ##
   ## Convert month
   ##
   unless ($month =~ /^\d+$/) {
      ##
      ## First try full month name
      ##
      my $new_month = undef;
      if (defined($MONTHS{$month})) {
         $new_month = $MONTHS{$month};
      ##
      ## Then try 3 char abbreviation
      ##
      } else {
         for (keys %MONTHS) {
            if (lc($month) eq lc(substr($_, 0, 3))) {
               $new_month = $MONTHS{$_};
               last;
            }
         } # FOR
      }
      CollectorCommon::shutdown("ERROR: Invalid month '$month' passed to convert_date") unless (defined($new_month));
      $month = $new_month;
   } # UNLESS
   ##
   ## Fix Year
   ##
   $year += 2000 if ($year < 100);   
   return (sprintf("%4d-%02d-%02d %02d:%02d:%02d", $year, $month, $day, $hour, $min, $sec),
           mktime(0, $min, $hour, $day, $month - 1, $year - 1900));
}

## ------------------------------------------------------
## send_email
## ------------------------------------------------------
sub send_email {
   my ($subject, $message, $no_queue) = @_;
   ##
   ## Actually Send Message
   ##
   if (defined($no_queue)) {
      $message .= "\n".
               "PROGRAM:  ".realpath($0)."\n".
               "HOSTNAME: $HOSTNAME\n".
               "COMMAND:  $0\n".
               "ARGS:     @ARGV\n";
   
      my $sendmail = '/usr/lib/sendmail';
      open(MAIL, "|$sendmail -oi -t");
      print MAIL "To: $SEND_EMAIL_TO\n";
      print MAIL "From: $SEND_EMAIL_FROM\n";
      print MAIL "Subject: $subject\n\n";
      print MAIL "$message\n";
      close(MAIL);
   ##
   ## Queue the message so we can send it in a batch
   ##
   } else {
      my %queue = ( "subject" => $subject,
                    "message" => $message );
      push(@MESSAGE_QUEUE, \%queue);
   }
}

## -----------------------------------------------------------
## validate
## -----------------------------------------------------------
sub validate {
   my ($html, %data) = @_;
   my @fields = ( "title", "user", "msg_date", "body" );
   foreach (@fields) {
      unless (defined($data{$_})) {
         my $missing = $_;
         my $error = "";
         foreach (@fields) {
            if ($missing ne $_) {
               my $temp = "$_:\t$data{$_}\n";
               print $temp;
               $error .= $temp;
            } else {
               my $temp = "ERROR: Missing $_";
               warn("$temp\n");
               $error .= $temp;
               last;
            }
         } # FOREACH
         $@ = $error;
         return (0);
      } # UNLESS
   } # FOREACH
   return (1);
}

## -----------------------------------------------------------
## find_symbol
## -----------------------------------------------------------
my @FINDER_STOCKS = ( );
my %FINDER_STOCKS_FULL_XREF = ( );
my @PENNY_STOCKS = ( );
sub find_symbol {
   my ($text) = @_;
   unless (defined($text) && length($text) > 0) {
      warn(debug_print("ERROR: Empty text passed to CollectorCommon::find_symbol"));
      return (undef);
   } # UNLESS
   
   ##
   ## On the first time we are called, we need to setup our giant
   ## list of stocks symbols so that we can try to figure out what
   ## the messages are talking about
   ##
   if (!%FINDER_STOCKS_FULL_XREF) {
      ##
      ## By calling db_connect after the main script, we will get
      ## the cache database connection. This way we don't have to pass
      ## any host information
      ##
      my $db = db_connect();
      my $sql = "SELECT stocks.id, ".
               "       stocks.symbol, ".
               "       IF(groups.suffix IS NOT NULL, CONCAT(stocks.symbol, groups.suffix), stocks.symbol) AS full_symbol ".
               "  FROM stocks, stock_groups, groups ".
               " WHERE stocks.id = stock_groups.stock_id ".
               "   AND groups.id = stock_groups.group_id";
      my $handle = $db->prepare($sql);
      $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
      while (my $row = $handle->fetchrow_hashref()) {
         push(@FINDER_STOCKS, $row->{'symbol'});
         $FINDER_STOCKS_FULL_XREF{$row->{'symbol'}} = $row->{'full_symbol'};
      }
      $handle->finish();

      $sql = "SELECT DISTINCT symbol FROM penny_stocks ";
      $handle = $db->prepare($sql);
      $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
      while (my $row = $handle->fetchrow_hashref()) {
         push(@PENNY_STOCKS, $row->{'symbol'});
      }
      $handle->finish();
   }
   ##
   ## Search for the stock symbol in a variety of ways with
   ## spaces around them. This makes it more accurate that we
   ## are actually getting the stock and not just part of it
   ##
   my @matches = ();
   foreach my $base_key (@FINDER_STOCKS) {
      ## Search the full symbol name first
      foreach my $key ($base_key, $FINDER_STOCKS_FULL_XREF{$base_key}) {
         if ($text =~ m/^$key[\s]+/) {
            push(@matches, $base_key) unless(grep(/^$base_key$/, @matches));
         } elsif ($text =~ m/[\s]+$key[\s]+/) {
            push(@matches, $base_key) unless(grep(/^$base_key$/, @matches));
         } elsif ($text =~ m/\($key\)/) {
            push(@matches, $base_key) unless(grep(/^$base_key$/, @matches));
         } elsif ($text =~ m/\*\*$key\*\*/) {
            push(@matches, $base_key) unless(grep(/^$base_key$/, @matches));
         }
      } # FOREACH
   } # FOREACH
   ##
   ## We didn't get anything!
   ##
   return (undef) unless ($#matches >= 0);
   ##
   ## Use the first penny stock that we find in our list of matches
   ##
   foreach my $match (@matches) {
      return ($match) if (is_penny($match));
   } # FOREACH
   ##
   ## Otherwise just return the first symbol we found. If it's not a penny
   ## stock, the code below us should take care of that problem
   ##
   return (shift(@matches));
}

## -----------------------------------------------------------
## is_penny
## -----------------------------------------------------------
sub is_penny {
   my ($symbol) = @_;
   return (defined($symbol) ? grep(/^$symbol$/, @PENNY_STOCKS) : 0);
}

1;
