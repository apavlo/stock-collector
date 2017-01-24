package StockInfo;

use strict;
use warnings;

use POSIX;
use DBI;
use List::Util;
use File::Basename;
use Cwd qw(realpath);
use CollectorCommon qw(debug_print);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(gather);

##
## Information URLs
##
our $YAHOO_INFO_URL   = "http://finance.yahoo.com/q?s=%s";
our $YAHOO_LOOKUP_URL = "http://finance.yahoo.com/lookup?s=%s";
our $YAHOO_DATA_URL     = "http://download.finance.yahoo.com/d/quotes.csv?s=%s&f=x&e=.csv";
our $YAHOO_SEC_URL      = "http://finance.yahoo.com/q/sec?s=%s";
our $YAHOO_STATS_URL    = "http://finance.yahoo.com/q/ks?s=%s";

##
## The unknown group is for any stocks that we can't find information for
##
my %EXCHANGE = ( 'XXX' => 1 );

## ------------------------------------------------------
## gather
## ------------------------------------------------------
sub gather {
   my ($stock_id, $symbol, $full_symbol, $name) = @_;
   my $db = CollectorCommon::db_connect();
   my ($sql, $url, $html) = (undef, undef, undef);

   ##
   ## Check whether it's an OB or a PK, or completely changed
   ##
   my $new_symbol = $symbol;
   my $new_name   = undef;
   my $new_exchange = undef;
   
   if ($symbol eq $full_symbol) {
      $url = sprintf($YAHOO_LOOKUP_URL, (defined($full_symbol) ? $full_symbol : $symbol));
      print debug_print("URL LOOKUP: $url") if (defined($main::opt_debug));
      my $html = CollectorCommon::wget_get_url($url);
   
      my @suffixes = ( "OB", "PK" );
      foreach my $suffix (@suffixes) {
         if ($html =~ m/<a href=".*?\/q.*?s=$symbol\.$suffix"[\s]*>$symbol\.$suffix<\/a>/) {
            $new_symbol .= ".$suffix";
            last;
         }
      } # FOR
   } else {
      $new_symbol = $full_symbol;
   }
   
   $url = sprintf($YAHOO_INFO_URL, $new_symbol);
   print debug_print("URL INFO: $url") if (defined($main::opt_debug));
   $html = CollectorCommon::wget_get_url($url);
   ##
   ## Quick check to see if the symbol changed on us
   ##
   if ($html =~ m/$new_symbol is no longer valid/i &&
       $html =~ m/It has changed to <a href=".*?">([\w\d\.]+)<\/a>/i) {
      $new_symbol = $1;
      ##
      ## We now need to mark that the symbol has changed, which then call us back
      ## and gather the new symbol's information. So we can just quit here.
      ##
      $stock_id = StockInfo::symbol_change($stock_id, $new_symbol);
      return;
   }
   my $info_html = $html;
   
   ##
   ## Check whether it's missing
   ##
   if ($html =~ m/There are no US &amp; Canada results for/ ||
       $html =~ /Get Quotes Results for \"$symbol.*?\"/) {
       return;
   }
   
   ##
   ## Things we're trying to collect
   ##
   my ($last_sec_filing, $avg_3mon, $avg_10day, $float, $outshares);

   ## --------------------------
   ## SEC Filings
   ## --------------------------
   $url = sprintf($YAHOO_SEC_URL, $new_symbol);
   print debug_print("URL SEC: $url") if (defined($main::opt_debug));
   my $data = CollectorCommon::wget_get_url($url);
   ## Check first if there is no data
   if ($data =~ m/There is no data available for $symbol/) {
      #$last_sec_filing = "0000-00-00";
   ##
   ## Pull out the edgar url, because it's easier to look at that page
   ## TODO: Make the SEC filings actual messages we store
   ##
   } elsif ($data =~ m/(http\:\/\/www\.edgar\-online\.com\/brand\/yahoo\/search\/\?cik=[\d]+)/) {
      $url = sprintf($1, $symbol);
      print debug_print("URL SEC FILING: $url") if (defined($main::opt_debug));
      $data = CollectorCommon::wget_get_url($url);
      ##
      ## Just grab the latest filing, that's all we care about for now
      ##
      if ($data =~ m/([\d]{1,2})\/([\d]{1,2})\/([\d]{0,4})\&nbsp\;/g) {
         $last_sec_filing = "$3-$1-$2";
      }
   }
   
   ## --------------------------
   ## Key Statistics
   ## --------------------------
   $url = sprintf($YAHOO_STATS_URL, $new_symbol);
   print debug_print("URL STATS: $url") if (defined($main::opt_debug));
   $data = CollectorCommon::wget_get_url($url);
   ## Average (3 month)
   if ($data =~ m/Average Volume \(3 month\)<font size=\"-1\"><sup>[\d]+<\/sup><\/font>:<\/td><td class=\"yfnc_tabledata1\">([,\.\d]*?)<\/td>/) {
      $avg_3mon = $1;
      $avg_3mon =~ s/,//g;
   }
   ## Average (10 day)
   if ($data =~ m/Average Volume \(10 day\)<font size=\"-1\"><sup>[\d]+<\/sup><\/font>:<\/td><td class=\"yfnc_tabledata1\">([,\.\d]*?)<\/td>/) {
      $avg_10day = $1;
      $avg_10day =~ s/,//g;
   }
   ## Float Shares
   if ($data =~ m/Float:<\/td><td class=\"yfnc_tabledata1\">(.*?)<\/td>/) {
      $float = convert_number($1);
   }
   ## Outstanding Shares
   if ($data =~ m/Shares Outstanding<font size=\"-1\"><sup>[\d]+<\/sup><\/font>:<\/td><td class=\"yfnc_tabledata1\">(.*?)<\/td>/) {
      $outshares = convert_number($1);
   }
   
   ## --------------------------
   ## Exchange
   ## --------------------------
   if (!defined($new_exchange)) {
      $url = sprintf($YAHOO_DATA_URL, $new_symbol);
      print debug_print("URL EXCHANGE: $url") if (defined($main::opt_debug));
      $data = CollectorCommon::wget_get_url($url);
      $data =~ s/\r\n$//g;
      $new_exchange = substr($data, 1, length($data) - 2);
      
      ##
      ## Sometimes Yahoo tells us the exchange is "N/A" when the main
      ## information page says that is something else. We'll do a quick check
      ## here to make sure that we are getting the right data
      ##
      if ($new_exchange eq "N/A" && $info_html =~ m/[\s]+\(([\w\s]+):$new_symbol\)[\s]+/) {
         $new_exchange = $1;
         chomp($new_exchange);
         print debug_print("FOUND BETTER EXCHANGE: $new_exchange");
      }
      
      ##
      ## If we don't have an group id cached for this exchange
      ##
      unless (defined($EXCHANGE{$new_exchange})) {
         $sql = "SELECT id FROM groups WHERE symbol = ".$db->quote($new_exchange);
         my $handle = $db->prepare($sql);
         $handle->execute() || die(debug_print($DBI::errstr));
         ##
         ## Add the group to the cache
         ##
         if (my $row = $handle->fetchrow_hashref()) {
            $EXCHANGE{$new_exchange} = $row->{'id'};
         ##
         ## Insert a new group
         ##
         } else {
            $sql = "INSERT INTO groups VALUES (null, ".$db->quote($new_exchange).", null, null, null)";
            $handle = $db->prepare($sql);
            print debug_print($sql) if (defined($main::opt_debug));
            $handle->execute() || die(debug_print($DBI::errstr));
            $EXCHANGE{$new_exchange} = $handle->{mysql_insertid};
         }
      } # UNLESS
   } # IF
   if (defined($main::opt_group)) {
      $sql = "UPDATE stock_groups ".
             "   SET group_id = ".$db->quote($EXCHANGE{$new_exchange}).
             " WHERE stock_id = ".$db->quote($stock_id);
   } else {
      $sql = "INSERT INTO stock_groups VALUES (".
               $db->quote($EXCHANGE{$new_exchange}).", ".
               $db->quote($stock_id).
             ")";
   }
   print debug_print($sql) if (defined($main::opt_debug));
   unless ($db->do($sql)) {
      ##
      ## It's not really an error if it's a duplicate entry
      ##
      unless ($DBI::err == $CollectorCommon::DBI_DUPLICATE_ERROR_CODE) {
         print debug_print($DBI::errstr);
         print debug_print($sql);
         exit;
      } # UNLESS
   } # UNLESS
   
   print "$name [$symbol]\n".
         "  Last Filing: ".(defined($last_sec_filing) ? $last_sec_filing : "-")."\n".
         "  Avg 10Day:   ".(defined($avg_10day) ? $avg_10day : "-")."\n".
         "  Avg 3Mon:    ".(defined($avg_3mon)  ? $avg_3mon : "-")."\n".
         "  Float:       ".(defined($float)     ? $float : "-")."\n".
         "  O.Shares:    ".(defined($outshares) ? $outshares : "-")."\n".
         "  Exchange:    ".(defined($new_exchange) ? $new_exchange : "-")."\n".
         "\n";
   
   if (defined($last_sec_filing) || defined($avg_10day) || defined($avg_3mon) ||
       defined($float) || defined($outshares)) {
      $sql = "UPDATE stocks SET ".
            "       industry_id = industry_id ".
            (defined($last_sec_filing) ? ", last_sec_filing = ".$db->quote($last_sec_filing) : "").
            (defined($avg_10day) ? ", avg_volume_10day = ".$db->quote(int($avg_10day)) : "").
            (defined($avg_3mon)  ? ", avg_volume_3mon = ".$db->quote(int($avg_3mon)) : "").
            (defined($float)     ? ", float_shares = ".$db->quote($float) : "").
            (defined($outshares) ? ", outstanding_shares = ".$db->quote($outshares) : "").
            " WHERE id = ".$db->quote($stock_id);
      print debug_print($sql) if (defined($main::opt_debug));
      $db->do($sql) || die(debug_print($DBI::errstr));
   }
   return;
}

## ------------------------------------------------------
## convert_number
## ------------------------------------------------------
sub convert_number {
   my ($ret) = @_;
   if ($ret =~ m/([\d\.]+)([MBKT])/) {
      $ret = $1;
      if ($2 eq "T") {
         $ret *= 1000000000000;
      } elsif ($2 eq "M") {
         $ret *= 1000000;
      } elsif ($2 eq "B") {
         $ret *= 1000000000;
      } elsif ($2 eq "K") {
         $ret *= 1000;
      }  
   }
   return ($ret);
}

## -----------------------------------------------------------
## symbol_change
## -----------------------------------------------------------
sub symbol_change {
   my ($old_id, $new_full_symbol, $no_gather) = @_;
   
   my $new_symbol = (split(/\./, $new_full_symbol))[0];
   my $new_id = undef;
   my $switch_exchange = undef;

   my $db = CollectorCommon::db_connect();
   my $sql = "SELECT stocks.symbol, ".
             "       IF(groups.suffix IS NOT NULL, CONCAT(stocks.symbol, groups.suffix), stocks.symbol) AS full_symbol ".
             "  FROM stocks, stock_groups, groups ".
             " WHERE stocks.id = stock_groups.stock_id ".
             "   AND groups.id = stock_groups.group_id ".
             "   AND stocks.valid = 1 ".
             "   AND stocks.id = ".$db->quote($old_id);
   my $handle = $db->prepare($sql);
   $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
   if (my $row = $handle->fetchrow_hashref()) {
      my $symbol      = $row->{'symbol'};
      my $full_symbol = $row->{'full_symbol'};
      $handle->finish();
      
      print debug_print("Investigating whether $full_symbol has switched to $new_full_symbol");
   
      ##
      ## We now need to check whether we already have a stock entry for this, because we
      ## didn't used to check whether the symbol had changed
      ##
      $sql = "SELECT stocks.id FROM stocks WHERE symbol = ".$db->quote($new_symbol);
      $handle = $db->prepare($sql);
      $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
      if ($row = $handle->fetchrow_hashref()) {
         $new_id = $row->{'id'};
         
         ##
         ## Important! Check whether the ids are the same. That means 
         ## the stock just switched exchanges!
         ##
         if ($new_id == $old_id) {
            print debug_print("Stock $symbol has switched exchanges");
            $sql = "DELETE FROM stock_groups WHERE stock_id = ".$db->quote($old_id);
            $handle = $db->prepare($sql);
            $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
            
            StockInfo::gather($new_id, $new_symbol, $new_full_symbol, "");
            $switch_exchange = 1;
         }
      } else {
         $handle->finish();
         $sql = "INSERT INTO stocks (".
                  "   name, ".
                  "   symbol, ".
                  "   valid ".
                  ") VALUES (".
                  $db->quote("").", ".
                  $db->quote($new_symbol).", ".
                  $db->quote("1").
                  ")";
         $handle = $db->prepare($sql);
         print debug_print($sql) if (defined($main::opt_debug));
         unless ($handle->execute()) {
            print debug_print($sql) unless (defined($main::opt_debug));
            CollectorCommon::shutdown($DBI::errstr);
         } # UNLESS
         $new_id = $handle->{mysql_insertid};
         
         ##
         ## Gather new information about this stock
         ##
         StockInfo::gather($new_id, $new_symbol, $new_full_symbol, "");
      }
      $handle->finish();
      
      CollectorCommon::shutdown("Unable to get new id for switching symbol $symbol to $new_symbol") unless (defined($new_id));
      
      ##
      ## Create a new stock_switch entry and mark the old stock as invalid
      ##
      unless (defined($switch_exchange)) {
         $sql = "INSERT INTO stock_switch (".
                  " old_id, new_id ".
                  ") VALUES (".
                  $db->quote($old_id).", ".$db->quote($new_id).")";
         $handle = $db->prepare($sql);
         print debug_print($sql) if (defined($main::opt_debug));
         $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
      
         $sql = "UPDATE stocks SET valid = 0 WHERE id = ".$db->quote($old_id);
         $handle = $db->prepare($sql);
         print debug_print($sql) if (defined($main::opt_debug));
         $handle->execute() || CollectorCommon::shutdown($DBI::errstr);
      } # UNLESS
      
      print debug_print("Stock $full_symbol has switched to $new_full_symbol");
   }
}
