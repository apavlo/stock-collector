#!/usr/bin/env perl
use strict;
use warnings;
no strict "refs"; 

BEGIN {
   my $arch = `uname -m`; chomp($arch);
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/share/perl/5.10.0");
   push(@INC, "/home/pavlo/.perl-$arch/lib/perl5");
   push(@INC, "/home/pavlo/Documents/Stocks/collector/modules");
}

use POSIX;
use DBI;
use List::Util;
use Getopt::Long;
use IO::Handle;
use FileHandle;
use Switch;

##
## Collection Modules
##
use CollectorCommon qw(debug_print);
use RagingBull;
use YahooMessageBoards;
use YahooGroups;
use InvestorHub;
use SocialPicks;
use GoogleFinance;
use AllStocks;
use TheLion;
use InvestorVillage;
use HotStockMarket;
use HotPennyStocks;
use SiliconInvestor;

my $SLEEP_SEED = 6;
srand();

##
## The number of consecutive errors before we report that something is wrong!
##
my $CONSECUTIVE_ERROR_LIMIT = 4;

##
## These are the sources they want to spider
##
my @TARGET_SOURCES = ( );

##
## Lookup table for function names
##
my %SOURCE_IDX         = ( );
my %SOURCE_URL_PREFIX  = ( );
my %SOURCE_BASE_URLS   = ( );
my %SOURCE_USE_SUFFIX  = ( );
my %SOURCE_UNIQUE_URLS = ( );
my %SOURCE_GROUP_URLS  = ( );
my %SOURCE_FETCHER     = ( );
my %STOCK_ID_XREF      = ( );

GetOptions("dbhost=s"    => \$main::opt_dbhost,
           "limit=s"     => \$main::opt_limit,
           "offset=s"    => \$main::opt_offset,
           "msglimit=s"  => \$main::opt_msglimit,
           "dburls"      => \$main::opt_dburls,
           "stdout=s"    => \$main::opt_stdout,
           "stderr=s"    => \$main::opt_stderr,
           "stop=s"      => \$main::opt_stopdate,
           "url=s"       => \$main::opt_url,
           "message=s"   => \@main::opt_messages,
           "source=s"    => \@TARGET_SOURCES,
           "validate"    => \$main::opt_validate,
           "nogroups"    => \$main::opt_nogroups,
           "nostocks"    => \$main::opt_nostocks,
           "noemail"     => \$main::opt_noemail,
           "nostop"      => \$main::opt_nostop,
           "sleep=s"     => \$SLEEP_SEED,
           "lock"        => \$main::opt_lock,
           "force"       => \$main::opt_force,
           "errorlimit"  => \$CONSECUTIVE_ERROR_LIMIT,
           "errordump=s" => \$main::opt_errordump,
           "debug"       => \$main::opt_debug,
           "debugfile=s" => \$main::opt_debugfile,
           ##
           ## Fix Options
           ##
           "fix-parents" => \$main::opt_fixparents,
           "fix-status"  => \$main::opt_fixstatus,
           "fix-user"    => \$main::opt_fixuser,
);
##
## OLD: If they don't provide a stop date, we will stop on Oct 1, 2007 (1191211200)
## NEW: Stop on Jan 1, 2008 (1199163600) since this is the stock data we have
##
$main::opt_stopdate = 1199163600 unless (defined($main::opt_stopdate));

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
## If we ever get this marker, force a geturl() call
##
my $FORCE_GET_URL = "XXXXX";

unless (defined($main::opt_dbhost)) {
   if (defined($ENV{'MYSQL_HOST'})) {
      $main::opt_dbhost = $ENV{'MYSQL_HOST'};
   } else {
      $main::opt_dbhost = "localhost";
   }
} ## UNLESS
my $db = CollectorCommon::db_connect($main::opt_dbhost);

## ------------------------------------------------------
## So here we are...
## We can either plow through all our stocks, or go through
## the urls that we have already spidered and update things
##
## The update_urls is a lookup table from the URL to the original message id
## This prevents us from trying to insert it twice.
## ------------------------------------------------------
my %SOURCE_STOCK_URLS = ( );
my %update_urls = ( );

##
## Grab our source information
##
my $sql = "SELECT sources.* ".
          "  FROM sources ".
          " WHERE sources.enabled = 1 ".
          ($#TARGET_SOURCES >= 0 ? " AND sources.id IN (".join(",", map { $db->quote($_) } @TARGET_SOURCES).")" : "");
print debug_print($sql) if (defined($main::opt_debug));
my $handle = $db->prepare($sql);
$handle->execute() || CollectorCommon::shutdown($DBI::errstr);
while (my $row = $handle->fetchrow_hashref()) {
   $SOURCE_IDX{$row->{'id'}}         = $row->{'name'};
   $SOURCE_URL_PREFIX{$row->{'id'}}  = $row->{'prefix_url'};
   $SOURCE_USE_SUFFIX{$row->{'id'}}  = $row->{'use_suffix'};
   $SOURCE_UNIQUE_URLS{$row->{'id'}} = $row->{'unique_url'};
   $SOURCE_GROUP_URLS{$row->{'id'}}  = ($row->{'group_url'} ? $row->{'group_url'} : undef);
   $SOURCE_BASE_URLS{$row->{'id'}}   = $row->{'base_url'};
   $SOURCE_FETCHER{$row->{'id'}}     = $row->{'fetcher'};
   my %hash = ( );
   $SOURCE_STOCK_URLS{$row->{'id'}} = \%hash;
}
$handle->finish();
CollectorCommon::shutdown("No sources were found for collection") unless (scalar keys(%SOURCE_IDX) > 0);

##
## TODO: SPECIAL: Fix Parents
##
if (defined($main::opt_fixparents)) {
   $sql = "SELECT messages.id AS parent_id, message_parents.child_id AS child_id ".
          "  FROM message_parents, messages ".
          " WHERE message_parents.parent_url = messages.url ";
   CollectorCommon::shutdown();
}
##
## SPECIAL: Fix message_url status codes
## Sometimes we correctly identify a URL to not be a penny stock, only
## to have it updated later with a status zero. I believe this logic 
## has been corrected, but this little code block will clean the data
## just in case some problems linger.
##
if (defined($main::opt_fixstatus)) {
   $sql = "SELECT message_urls.url, message_urls.stock_id ".
          "  FROM message_urls ".
          "  LEFT OUTER JOIN penny_stocks ON penny_stocks.id = message_urls.stock_id ".
          " WHERE penny_stocks.id IS NULL ".
          "   AND message_urls.status != 4 ".
          "   AND message_urls.stock_id > 1 ".
          ($#TARGET_SOURCES >= 0 ? " AND sources.id IN (".join(",", map { $db->quote($_) } @TARGET_SOURCES).")" : "");
   print debug_print($sql) if (defined($main::opt_debug));
   $handle = $db->prepare($sql);
   $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
   while (my $row = $handle->fetchrow_hashref()) {
      $sql = "UPDATE message_urls ".
             "   SET status = ".$db->quote($CollectorCommon::MSG_STATUS_NOTPENNY).
             " WHERE url = ".$db->quote($row->{'url'}).
             "   AND stock_id = ".$db->quote($row->{'stock_id'});
      print debug_print($sql) if (defined($main::opt_debug));
      $db->do($sql) || CollectorCommon::shutdown($DBI::errstr);
   } # WHILE
   $handle->finish();
   CollectorCommon::shutdown();
}

##
## Database Messages
## Sometimes we want to reload messages that already exist in the database
##
if (defined($main::opt_dburls) || defined(@main::opt_messages) ||
    defined($main::opt_fixparents) || defined($main::opt_fixuser)) {
   foreach my $source_id (sort keys %SOURCE_BASE_URLS) {
      next unless (defined($SOURCE_IDX{$source_id}));
      $sql = "SELECT messages.id, url, penny_stocks.id AS stock_id, symbol, full_symbol ".
             "  FROM messages, penny_stocks ".
             " WHERE messages.stock_id = penny_stocks.id ";
      ##
      ## Fix Parents
      ## Reparse all the messages that have their parent as NULL
      ## to see whether we can correctly go back and get their parents
      ##
      if (defined($main::opt_fixparents)) {
         $sql .= "   AND messages.parent_id IS NULL ".
                 "   AND messages.source_id = ".$db->quote($source_id);
      ##
      ## Fix Users
      ## RagingBull has a ton of messages where 'USER' is the user
      ##
      } elsif (defined($main::opt_fixuser)) {
         $sql .= "   AND messages.user = 'USER' ".
                 "   AND messages.source_id = 2 ";
      ##
      ## Reload Messages
      ##
      } elsif (defined(@main::opt_messages)) {
         $sql .= "   AND messages.id IN (".join(",", map { $db->quote($_) } @main::opt_messages).")";
      ##
      ## Parse Hotpenny Messages
      ## These are messages where we only know that they exist and
      ## are talking about a specific stock. We haven't actually gone
      ## and collected their information yet
      ##
      } else {
         $sql .= "   AND messages.url LIKE ".$db->quote("\%".$SOURCE_URL_PREFIX{$source_id}."\%").
                 "   AND messages.source_id = 4 ";
      }
      $sql .= (defined($main::opt_limit) ? " LIMIT $main::opt_limit" : "");
      
      $handle = $db->prepare($sql);
      print debug_print($sql);
      $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
      while (my $row = $handle->fetchrow_hashref()) {
         my $url = $row->{'url'};
         my $id = $row->{'id'};
         ##
         ## Important! Some crappy sites have separate listings for the
         ## full symbol and the regular symbol. So we need to basically add the 
         ## symbol twice to our list. Terrible...
         ##
         my @symbols = ($SOURCE_USE_SUFFIX{$source_id} > 0 ? $row->{'full_symbol'} : $row->{'symbol'});
         push(@symbols, $row->{'symbol'}) if ($SOURCE_USE_SUFFIX{$source_id});
         foreach my $symbol (@symbols) {
            unless (defined($SOURCE_STOCK_URLS{$source_id}->{$symbol})) {
               my @array = ( );
               $SOURCE_STOCK_URLS{$source_id}->{$symbol} = \@array;
            } # UNLESS
            push(@{$SOURCE_STOCK_URLS{$source_id}->{$symbol}}, $url) unless (grep(/$url/, @{$SOURCE_STOCK_URLS{$source_id}->{$symbol}}));
         } # FOREACH
         $update_urls{$url} = $id;
         
         $STOCK_ID_XREF{$row->{'full_symbol'}} = $row->{'stock_id'};
         $STOCK_ID_XREF{$row->{'symbol'}} = $row->{'stock_id'};
      } # WHILE
   } # FOREACH
##
## Otherwise...
##
} else {
   $sql = "SELECT penny_stocks.id AS stock_id, symbol, full_symbol ".
          "  FROM penny_stocks ".
          ($#ARGV >= 0 ? " WHERE symbol IN (".join(",", map { $db->quote($_) } @ARGV).")" : "").
          (defined($main::opt_limit) ? " LIMIT $main::opt_limit" : "").
          (defined($main::opt_offset) ? " OFFSET $main::opt_offset" : "");
   print debug_print($sql) if (defined($main::opt_debug));
   $handle = $db->prepare($sql);
   $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
   while (my $row = $handle->fetchrow_hashref()) {
      ##
      ## For each source, create a list of urls that we want to look at
      ##
      foreach my $source_id (keys %SOURCE_BASE_URLS) {
         ##
         ## Important! Some crappy sites have separate listings for the
         ## full symbol and the regular symbol. So we need to basically add the 
         ## symbol twice to our list. Terrible...
         ##
         my @symbols = ($SOURCE_USE_SUFFIX{$source_id} > 0 ? $row->{'full_symbol'} : $row->{'symbol'});
         push(@symbols, $row->{'symbol'}) if ($SOURCE_USE_SUFFIX{$source_id} == 2);
         foreach my $symbol (@symbols) {
            unless (defined($SOURCE_STOCK_URLS{$source_id}->{$symbol})) {
               my @array = ( );
               $SOURCE_STOCK_URLS{$source_id}->{$symbol} = \@array;
            } # UNLESS
            my $url = (defined($main::opt_url) ? $main::opt_url : sprintf($SOURCE_BASE_URLS{$source_id}, $symbol));
            push(@{$SOURCE_STOCK_URLS{$source_id}->{$symbol}}, $url) unless (grep(/$url/, @{$SOURCE_STOCK_URLS{$source_id}->{$symbol}}));
         } # FOREACH
      } # FOREACH
      $STOCK_ID_XREF{$row->{'full_symbol'}} = $row->{'stock_id'};
      $STOCK_ID_XREF{$row->{'symbol'}} = $row->{'stock_id'};
   } # WHILE
   $handle->finish();
   
   ##
   ## Important! Some sources are not sorted by symbol. So we are just going to
   ## set dummy symbols that represent the different groups we want to look at
   ## We will have logic that makes sure we don't try to insert on these
   ##
   foreach my $source_id (keys %SOURCE_IDX) {
      next unless (defined($SOURCE_GROUP_URLS{$source_id}) && defined($SOURCE_STOCK_URLS{$source_id}));
      print debug_print("Retrieving groups for $SOURCE_IDX{$source_id}") if (defined($main::opt_debug));
      ##
      ## Only clear our existing URLS for some sites
      ## Some sites have both stock urls and group urls
      ##
      %{$SOURCE_STOCK_URLS{$source_id}} = ( ) if ($SOURCE_GROUP_URLS{$source_id} == 1 || defined($main::opt_nostocks));
      
      ##
      ## Add the group urls unless we were asked not to
      ##
      unless ($main::opt_nogroups) {
         my $func_groups  = "$SOURCE_IDX{$source_id}::groups";
         my ($use_base_url, @groups) = &$func_groups();
         foreach my $group (@groups) {
            my $url = ($use_base_url ? sprintf($SOURCE_BASE_URLS{$source_id}, $group) : $group);
            $url = $main::opt_url if (defined($main::opt_url));
            my @array = ( $url );
            $SOURCE_STOCK_URLS{$source_id}->{"GROUP:$group"} = \@array;
         } # FOREACH
         print debug_print("Retrieved ".($#groups+1)." groups for $SOURCE_IDX{$source_id}") if (defined($main::opt_debug));
      } # UNLESS
   } # FOREACH
}

## ------------------------------------------------------
## Let's roll...
## At the very top we are going to spider each source one at a time
## For each source we'll look at all the symbols. And for each symbol we look
## at all the urls and try to gather more.
## ------------------------------------------------------
foreach my $source_id (sort keys %SOURCE_STOCK_URLS) {
   next unless (defined($SOURCE_IDX{$source_id}));
   my $source_key = $SOURCE_IDX{$source_id};
   $source_key =~ s/\.//g;
   my $func_parse    = "${source_key}::parse";
   my $func_notfound = "${source_key}::notfound";
   my $func_islist   = "${source_key}::islist";
   my $func_geturls  = "${source_key}::geturls";
   my $func_fetcher  = "CollectorCommon::${SOURCE_FETCHER{$source_id}}_get_url";
   
   ##
   ## LOCK
   ##
   if (defined($main::opt_lock) && $main::opt_lock) {
      unless (CollectorCommon::lock($source_id)) {
         print debug_print("Failed to acquire lock for Source ID \#$source_id. Skipping...");
         next;
      } # UNLESS
   }
   
   print debug_print("Current Source ID = $source_id");
   
   ##
   ## Spider each symbol until we run out of things to grab
   ## We keep track of the oldest message that we found. This will
   ## prevent us from going too far back in time and grabbing a bunch of data we don't want
   ##
   foreach my $symbol (List::Util::shuffle keys %{$SOURCE_STOCK_URLS{$source_id}}) {
      my $last_timestamp = undef;
      my $stock_id = $STOCK_ID_XREF{$symbol};
      my $symbol_col = "symbol";
      if ($SOURCE_USE_SUFFIX{$source_id} > 0) {
         $symbol_col = "full_${symbol_col}" if ($symbol =~ /\.[\w]+$/ && $symbol !~ /^GROUP:.*/);
      }
      print debug_print("STOCK: $symbol [$symbol_col]");
      
      my $last_list_url;
      my $base_url;
      my $error_count = 0;
      my $orig_symbol = $symbol;
      my %parent_urls = ( );
      my $first_url = 1;
      foreach my $url (@{$SOURCE_STOCK_URLS{$source_id}->{$orig_symbol}}) {
         $base_url = $url unless (defined($base_url));
         my $force_fetch = ($url eq $FORCE_GET_URL);
         sleep(int(rand($SLEEP_SEED)) + int(rand($SLEEP_SEED))) unless ($force_fetch);
         
         ##
         ## Reset the symbol if this is a group board url
         ## This forces the parsing function to figure out what the symbol is for us.
         ##
         my $group_url = ($orig_symbol =~ /^GROUP:.*/);
         $symbol = ($group_url ? undef : $orig_symbol);
         
         ##
         ## Check whether we should skip. This is kind of screwy having this here twice...
         ##
         if ($first_url) {
            $sql = "SELECT url, (NOW() - cdate) AS last_visited ".
                  "  FROM message_urls ".
                  " WHERE url = ".$db->quote($url).
                  "   AND status = ".$db->quote($CollectorCommon::MSG_STATUS_INVALID).
                  ##
                  ## Don't bother looking up the symbol if there isn't one!
                  ##
                  (defined($symbol) ? " AND stock_id = ".$db->quote(get_stock_id($symbol, $symbol_col)) : "");
            print debug_print($sql) if (defined($main::opt_debug));
            $handle = $db->prepare($sql);
            $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
            if (my $row = $handle->fetchrow_hashref()) {
               ## Check whether we have visited this first page in the last seven days
               if ($row->{'last_visited'} < 604800 && !defined($main::opt_force)) {
                  print debug_print("[SKIP] $url") if (defined($main::opt_debug));
                  next;
               }
            } # UNLESS
            $handle->finish();
            $first_url = 0;
         } ## IF (first_url)
         
         ##
         ## Grab the page
         ## Yahoo Groups has to use elinks!
         ## Check if there is nothing there for us
         ## If it's the force geturl marker, then its not a real page for us to grab
         ##
         my $html = "";
         unless ($force_fetch) {
            ##
            ## Debug File
            ##
            if (defined($main::opt_debugfile)) {
               CollectorCommon::shutdown("ERROR: The debug file '$main::opt_debugfile' does not exist") unless (-f $main::opt_debugfile);
               print debug_print("Reading from file '$main::opt_debugfile'") if (defined($main::opt_debug));
               $html = `cat $main::opt_debugfile`;
            ##
            ## Fetch file from the web!
            ##
            } else {
               #print debug_print("Fetching HTML from source: $url") if (defined($main::opt_debug));
               $html = &$func_fetcher($url);
            }
            if (&$func_notfound($html, $symbol, $stock_id)) {
               print debug_print("[INVALID] $url");
               log_url($url, $source_id, undef, $symbol, $CollectorCommon::MSG_STATUS_INVALID);
#                print "$html\n";
               next;
            }
         } # UNLESS
         ##
         ## Sometimes we get no HTML?
         ##
         unless (length($html) > 0) {
            warn(debug_print("ERROR: No HTML collected?"));
            #die(debug_print("ERROR: No HTML collected?"));
            next;
         } # UNLESS

         ##
         ## Message List
         ## We can call our specific function to determine whether we are looking
         ## at a page with a list of messages
         ## 
         if ($force_fetch || &$func_islist($html, $symbol, $stock_id)) {
            print debug_print($force_fetch ? "Force a URL fetch!" : "[LIST] $url");
            ##
            ## If this is a message list page, then that means we have successfully parsed all the
            ## messages that were on the previous list page. We need to log the last list page
            ## so that we don't visit it again. This ensures that if there is an error, we'll come back
            ## to the previous page
            ##
            #if (defined($last_list_url) && ($base_url ne $last_list_url)) {
            #   print "\$last_list_url: $last_list_url\n".
            #         "\$base_url:      $base_url\n";
            #   log_url($last_list_url, $symbol, $symbol_col, 0);
            #   exit;
            #}
            $last_list_url = $url;
            ##
            ## Only add these urls if we haven't seen a message past our timestamp yet
            ##
            if ($force_fetch || !defined($last_timestamp) || (defined($last_timestamp) && $last_timestamp >= $main::opt_stopdate)) {
               my @urls = &$func_geturls($html, $symbol, $stock_id);
               ##
               ## If this is the base_url and we didn't get any urls, then somebody
               ## changed and we need to be alerted!
               ## Stop everything we have hit too many consecutive errors
               ##
               if ($base_url eq $url && $#urls == -1) {
                  $error_count++;
                  print debug_print("Consecutive Base URL Errors: $error_count") if (defined($main::opt_debug));
                  if ($error_count > $CONSECUTIVE_ERROR_LIMIT) {
                     my $subject = "ERROR: Unable to parse urls for $SOURCE_IDX{$source_id}";
                     my $message = "We were unable to gather message urls from $SOURCE_IDX{$source_id}\[$source_id\] ".
                                 "base url for ".
                                 (defined($symbol) ? "stock $symbol" : "group $orig_symbol").
                                 ".\n$url";
                     $message .= "\nDump output written to '$main::opt_errordump'" if (defined($main::opt_errordump));
                     CollectorCommon::send_email($subject, $message) unless (defined($main::opt_noemail));
                     CollectorCommon::file_dump($html, $main::opt_errordump) if (defined($main::opt_errordump));
                     CollectorCommon::shutdown($subject) unless (defined($main::opt_nostop));
                     $error_count = 0;
                  }
                  next;
               }
               ##
               ## Prune out the urls that we have already visited
               ##
               if ($SOURCE_UNIQUE_URLS{$source_id} && $#urls >= 0) {
                  ##
                  ## TODO: Skip URLs that we have visited in the last X number of hours/days
                  ##
                  $sql = "SELECT url ".
                         "  FROM message_urls ".
                         " WHERE url IN (".join(",", map { $db->quote($_) } @urls).") ".
                         ##
                         ## Don't bother looking up the symbol if there isn't one!
                         ##
                         (defined($symbol) ? " AND stock_id = ".$db->quote(get_stock_id($symbol, $symbol_col)) : "").
                         "   AND status IN (".join(",", map { $db->quote($_) } ($CollectorCommon::MSG_STATUS_OK, $CollectorCommon::MSG_STATUS_NOSYMBOL, $CollectorCommon::MSG_STATUS_OLD, $CollectorCommon::MSG_STATUS_NOTPENNY, $CollectorCommon::MSG_STATUS_URLCHANGE)).")";
                  #print debug_print($sql) if (defined($main::opt_debug));
                  $handle = $db->prepare($sql);
                  $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
                  while (my $row = $handle->fetchrow_hashref()) {
                     ##
                     ## Slutty code...
                     ##
                     for (my $ctr = 0; $ctr <= $#urls; $ctr++) {
                        if ($urls[$ctr] eq $row->{'url'}) {
                           #print debug_print("SKIPPING: $urls[$ctr]");
                           splice(@urls, $ctr, 1);
                           last;
                        }
                     } # FOR
                  } # WHILE
                  $handle->finish();
               } # IF UNIQUE URLS
               ##
               ## Note that we use the orig_symbol here
               ## This makes sure that we are always adding to the correct list
               ## Sometimes we get a ton of dupes... not sure if this will fix it...
               ##
               foreach my $new_url (@urls) {
                  push(@{$SOURCE_STOCK_URLS{$source_id}->{$orig_symbol}}, $new_url) unless (grep(/^$new_url$/, @{$SOURCE_STOCK_URLS{$source_id}->{$orig_symbol}}) || $url eq $new_url);
               } # FOREACH
               #foreach (@urls) {
               #   print debug_print("[QUEUE] $_");
               #}
            } # IF
         ##
         ## Message Post
         ## We assume all other urls are going to have the message content that
         ## we will want to parse
         ##
         } else {
            print debug_print("[DATA] $url");
            my ($timestamp, @data) = &$func_parse($html, $symbol, $url);
            $last_timestamp = $timestamp; #  if (!defined($last_timestamp) || $timestamp < $last_timestamp);
            my $last_msg_date = undef;
            
            ##
            ## Make sure we only add the force geturl flag once per set of messages
            ##
            my $added_geturl = undef;
            
            ##
            ## A single page may contain multiple messages.
            ##
            for (my $ctr = 0; $ctr <= $#data; $ctr++) {
               my %d = %{$data[$ctr]};
               $last_msg_date = $d{'msg_date'};
               ##
               ## The parse function can correct us on what symbol it should be
               ## We will skip this message if we do not have a symbol. Technically this
               ##
               if (defined($d{'symbol'})) {
                  $symbol = $d{'symbol'};
                  delete $d{'symbol'};
               }
               unless (defined($symbol)) {
                  log_url($url, $source_id, $d{'msg_date'}, $symbol, $CollectorCommon::MSG_STATUS_NOSYMBOL);
                  next;
               } # UNLESS
               ##
               ## Check whether the stock is different than the original stock
               ## This means that we used the find_symbol() function to pull the symbol out
               ## and we need to check whether what we got back is an actual penny stock
               ##
               if ($orig_symbol ne $symbol && !CollectorCommon::is_penny($symbol)) {
                  log_url($url, $source_id, $d{'msg_date'}, $symbol, $CollectorCommon::MSG_STATUS_NOTPENNY);
                  next;
               } # UNLESS
               ##
               ## If we didn't get all the information we wanted, we'll log this URL 
               ## as an error so that we can come back later and take a look to see
               ## what went wrong
               ##
               if (!CollectorCommon::validate($html, %d) && !defined($main::opt_fixuser)) {
                  my $error = $@;
                  log_url($url, $source_id, $d{'msg_date'}, $symbol, $CollectorCommon::MSG_STATUS_ERROR);
                  if ($main::opt_validate) {
                     my $subject = "ERROR: Validation Error For $SOURCE_IDX{$source_id}";
                     my $message = "The following message failed to validate properly for $SOURCE_IDX{$source_id}\[$source_id\] ".
                                   "on ".(defined($symbol) ? "stock $symbol" : "group $orig_symbol").
                                   ".\n\n$url\n\n$error";
                     $message .= "\n\nDump output written to '$main::opt_errordump'" if (defined($main::opt_errordump));
                     CollectorCommon::send_email($subject, $message) unless (defined($main::opt_noemail));
                     CollectorCommon::file_dump($html, $main::opt_errordump) if (defined($main::opt_errordump));
                     CollectorCommon::shutdown($subject) unless (defined($main::opt_nostop));
                  }
                  last;
               } # IF
               ##
               ## Check if we were told that we need to call geturls again
               ## before we finish up with this symbol
               ##
               if (defined($d{'load_urls'})) {
                  delete $d{'load_urls'};
                  push(@{$SOURCE_STOCK_URLS{$source_id}->{$orig_symbol}}, $FORCE_GET_URL) unless (defined($added_geturl));
                  $added_geturl = 1;
               }
               
               ##
               ## Skip if this is message is too old
               ## We really shouldn't be logging the message as being too old
               ##
               if ($timestamp < $main::opt_stopdate) {
                  log_url($url, $source_id, $d{'msg_date'}, $symbol, $CollectorCommon::MSG_STATUS_OLD);
                  last;
               } # IF
               #CollectorCommon::debug_dump(%d);
               #exit;
               ##
               ## Figure out what the parent message is
               ##
               $d{"parent_id"} = undef unless (defined($d{"parent_id"}));
               my $parent_url = undef;
               if (defined($d{"parent"})) {
                  switch ($d{"parent"}) {
                     ##
                     ## Parent is the first messsage in the current list
                     ##
                     case ($CollectorCommon::PARENT_FIRST) {
                        $d{"parent_id"} = $data[0]->{"id"};
                        last;
                     }
                     ##
                     ## Get the parent from a URL
                     ## We're going to do the lookup right now, so that we
                     ## can check whether our parent has been seen yet. If not, 
                     ## then need to update ourselves after our parent is visited
                     ##
                     case ($CollectorCommon::PARENT_URL) {
                        $parent_url = $d{"parent_url"};
                        delete $d{"parent_url"};
                        last;
                     }
                     ##
                     ## Unknown
                     ##
                     else {
                        CollectorCommon::shutdown("ERROR: Invalid parent logic code ".$d{"parent"}." from $SOURCE_IDX{$source_id}");
                     }
                  } # SWITCH
                  delete $d{"parent"};
               }
               ##
               ## Update an existing record
               ##
               if (defined($update_urls{$url})) {
                  $sql = "UPDATE messages SET ".
                         "       source_id = ".$db->quote($source_id).", ".
                         "       cdate = CURRENT_TIMESTAMP ";
                  foreach (keys %d) {
                     $sql .= ", $_ = ".$db->quote($d{$_});
                  }
                  $sql .= " WHERE id = ".$db->quote($update_urls{$url});
               ##
               ## Insert a new record
               ##
               } else {
                  $sql = "INSERT INTO messages VALUES (".
                        "  NULL, ".
                        "  ".$db->quote(get_stock_id($symbol, $symbol_col)).", ".
                        "  ".$db->quote($d{'title'}).", ".
                        "  ".$db->quote($d{'body'}).", ".
                        "  ".$db->quote($d{'sentiment'}).", ".
                        "  ".$db->quote($url).", ".
                        "  ".$db->quote($d{'user'}).", ".
                        "  ".$db->quote($d{'msg_date'}).", ".
                        "  CURRENT_TIMESTAMP, ".
                        "  ".$db->quote($source_id).", ".
                        "  ".$db->quote($d{'parent_id'}).")";
               }
               print debug_print($sql) if (defined($main::opt_debug));
               $handle = $db->prepare($sql);
               unless ($handle->execute()) {
                  ##
                  ## It's not really an error if it's a duplicate entry
                  ##
                  unless ($DBI::err == $CollectorCommon::DBI_DUPLICATE_ERROR_CODE) {
                     print debug_print($sql) unless (defined($main::opt_debug));
                     CollectorCommon::shutdown($DBI::errstr);
                  } # UNLESS
               } # UNLESS
               ##
               ## Save the insert id in case we need it
               ##
               if (defined($update_urls{$url})) {
                  $data[$ctr]->{"id"} = $update_urls{$url};
                  print debug_print("Updated Message Record: ".$data[$ctr]->{"id"}) if (defined($main::opt_debug));
               } else {
                  $data[$ctr]->{"id"} = $handle->{mysql_insertid};
                  print debug_print("New Message Record: ".$data[$ctr]->{"id"}) if (defined($main::opt_debug));
               }
               $handle->finish();

               ##
               ## If we have no id, that means we were a dupe and we were successfully
               ## blocked by the DB! We want to check whether our URL is different from
               ## the existing record, which means that the site updated their format
               ##
               unless (defined($data[$ctr]->{"id"}) && $data[$ctr]->{"id"} > 0) {
                  $sql = "SELECT id ".
                         "  FROM messages ".
                         " WHERE stock_id = ".$db->quote(get_stock_id($symbol, $symbol_col)).
                         "   AND msg_date = ".$db->quote($d{'msg_date'}).
                         "   AND user = ".$db->quote($d{'user'}).
                         "   AND source_id = ".$db->quote($source_id).
                         "   AND url != ".$db->quote($url);
                  print debug_print($sql) if (defined($main::opt_debug));
                  $handle = $db->prepare($sql);
                  $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
                  if (my $row = $handle->fetchrow_hashref()) {
                     log_url($url, $source_id, $d{'msg_date'}, $symbol, $CollectorCommon::MSG_STATUS_URLCHANGE, $row->{'id'});
                     next;
                  }
                  $handle->finish();
               } # UNLESS
               
               ##
               ## See if we need to add an entry about this message
               ## needing a parent. We do two things here:
               ##    1) Check whether anybody was waiting on us
               ##    2) Check whether our parent exists. If not, then add a record
               ##
               my @parent_sql = ( );
               $sql = "SELECT child_id FROM message_parents WHERE parent_url = ".$db->quote($url);
               $handle = $db->prepare($sql);
               $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
               my @children = ( );
               while (my $row = $handle->fetchrow_hashref()) {
                  push(@children, $row->{'child_id'});
               } # WHILE
               $handle->finish();
               ##
               ## Update children
               ##
               if ($#children >= 0) {
                  $sql = "UPDATE messages ".
                         "   SET parent_id = ".$db->quote($data[$ctr]->{"id"}).
                         " WHERE id IN (".join(",", map { $db->quote($_) } @children).")";
                  push(@parent_sql, $sql);
                  $sql = "DELETE FROM message_parents ".
                         " WHERE parent_url = ".$db->quote($url).
                         "   AND child_id IN (".join(",", map { $db->quote($_) } @children).")";
                  push(@parent_sql, $sql);
               }
               ##
               ## Add our own record
               ##
               if ($parent_url) {
                  $sql = "SELECT id FROM messages ".
                         " WHERE url = ".$db->quote($parent_url).
                         "   AND source_id = ".$db->quote($source_id).
                         "   AND stock_id = ".$db->quote(get_stock_id($symbol, $symbol_col));
                  $handle = $db->prepare($sql);
                  print debug_print($sql) if (defined($main::opt_debug));
                  $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
                  if (my $row = $handle->fetchrow_hashref()) {
                     $sql = "UPDATE messages ".
                            "   SET parent_id = ".$db->quote($row->{'id'}).
                            " WHERE id = ".$db->quote($data[$ctr]->{"id"});
                     print debug_print($sql) if (defined($main::opt_debug));
                     push(@parent_sql, $sql);
                     $sql = "DELETE FROM message_parents ".
                            " WHERE child_id = ".$db->quote($data[$ctr]->{"id"});
                     push(@parent_sql, $sql);
                  } else {
                     $sql = "INSERT INTO message_parents VALUES (".
                           "  ".$db->quote($parent_url).", ".
                           "  ".$db->quote($data[$ctr]->{"id"}).", ".
                           "  NULL".
                           ")";
                     push(@parent_sql, $sql);
                  }
               }
               ##
               ## Execute all the SQL
               ##
               foreach my $sql (@parent_sql) {
                  print debug_print($sql) if (defined($main::opt_debug));
                  unless ($db->do($sql)) {
                     ##
                     ## It's not really an error if it's a duplicate entry
                     ##
                     unless ($DBI::err == $CollectorCommon::DBI_DUPLICATE_ERROR_CODE) {
                        print debug_print($sql);
                        CollectorCommon::shutdown($DBI::errstr);
                     } # UNLESS
                  } # UNLESS
               } # FOREACH
               log_url($url, $source_id, $last_msg_date, $symbol, $CollectorCommon::MSG_STATUS_OK) if (defined($symbol) && $timestamp >= $main::opt_stopdate);
            } # FOR
            if (defined($main::opt_msglimit)) {
               last if (--$main::opt_msglimit <= 0);
            }
            
         } # ELSE
      } # FOREACH
      ##
      ## Refresh lock...
      ##
      CollectorCommon::refresh($source_id);
   } # FOREACH
   ##
   ## Unlock
   ##
   CollectorCommon::unlock($source_id);
} # FOREACH
print debug_print("All done!") if (defined($main::opt_debug));
CollectorCommon::shutdown();

## ------------------------------------------------------
## log_url
## ------------------------------------------------------
sub log_url {
   my ($url, $source_id, $msg_date, $symbol, $status, $duplicate_id) = @_;
   $symbol =~ s/\.[\w]+$// if (defined($symbol));
   my ($sql, $handle);
   
   ##
   ## Get the stock_id
   ## Kind of convoluted, but oh well...
   ##
   my $stock_id = 0;
   $stock_id = get_stock_id($symbol) if (defined($symbol));
   $sql = "INSERT INTO message_urls VALUES ( ".
             "  ".$db->quote($url).", ".
             "  ".$db->quote($stock_id).", ".
             "  ".$db->quote($source_id).", ".
             "  ".$db->quote($status).", ".
             "  ".$db->quote($duplicate_id).", ".
             "  ".(defined($msg_date) ? $db->quote($msg_date) : "CURRENT_TIMESTAMP").", ".
             "  NULL )";
   print debug_print("[".(defined($symbol) ? $symbol : "???")."] $sql") if (defined($main::opt_debug));
   unless ($db->do($sql)) {
      ##
      ## It's not really an error if it's a duplicate entry
      ## We just need to update whether it was an error or not
      ##
      if ($DBI::err == $CollectorCommon::DBI_DUPLICATE_ERROR_CODE) {
         $sql = "UPDATE message_urls ".
                "   SET status = ".$db->quote($status).", ".
                "       msg_date = ".$db->quote($msg_date).", ".
                "       cdate = CURRENT_TIMESTAMP ".
                " WHERE url = ".$db->quote($url)." ".
                "   AND source_id = ".$db->quote($source_id)." ".
                "   AND stock_id IN (0, ".$db->quote($stock_id).")";
         print debug_print($sql) if (defined($main::opt_debug));
         $db->do($sql) || CollectorCommon::shutdown($DBI::errstr);
      ##
      ## Real Error
      ##
      } else {
         print debug_print($DBI::errstr);
         print debug_print($sql) unless (defined($main::opt_debug));
         CollectorCommon::shutdown();
      } # UNLESS
   ##
   ## Delete any records for this URL that don't have a stock_id
   ##
   } elsif (defined($symbol)) {
      $sql = "DELETE FROM message_urls ".
             " WHERE url = ".$db->quote($url)." ".
             "   AND stock_id = 0";
      print debug_print($sql) if (defined($main::opt_debug));
      $db->do($sql) || CollectorCommon::shutdown($DBI::errstr);
   }
   return;
}

## ------------------------------------------------------
## get_stock_id
## ------------------------------------------------------
sub get_stock_id {
   my ($symbol, $symbol_col) = @_;
   $symbol_col = "symbol" unless (defined($symbol_col));
   
   my $cache_symbol = $symbol;
   $cache_symbol =~ s/([\w]+)\.[\w]+/$1/;
   
   unless (defined($STOCK_ID_XREF{$cache_symbol})) {
      print debug_print("Symbol $cache_symbol not in cache. Looking up from database") if (defined($main::opt_debug));
      my $sql = "SELECT id FROM penny_stocks WHERE $symbol_col = ".$db->quote($symbol);
      $handle = $db->prepare($sql);
      $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
      if (my $row = $handle->fetchrow_hashref()) {
         $STOCK_ID_XREF{$cache_symbol} = $row->{'id'};
      } else {
         print debug_print($sql);
         CollectorCommon::shutdown("Stock $symbol was found in the database");
      }
      $handle->finish();   
   } else {
      print debug_print("Using cache for symbol $cache_symbol: $STOCK_ID_XREF{$cache_symbol}") if (defined($main::opt_debug));
   } # UNLESS
   return ($STOCK_ID_XREF{$cache_symbol});
}
