package YahooMessageBoards;

use strict;
use warnings;
use CollectorCommon qw(clean convert_date);
use StockInfo;

## ===========================================================
## Yahoo! Message Boards
## ===========================================================

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   my %data = ( );
   my $timestamp;
   
   ##
   ## Title
   ##
   if ($html =~ m/<span class="user-data"><B>(.*?)<\/B><\/span>/) {
      $data{'title'} = clean($1);
   }
   
   ##
   ## User
   ## Without 'title' tag
   ##
   if ($html =~ m/[\s]*<a href=\"http:\/\/messages\.finance\.yahoo\.com\/profile\?bn=.*?\" target=\"_new\" class=\"[\w\s]+\"[\s]*>(.*?)<\/a>/) {
      $data{"user"} = clean($1);
   ##
   ## With 'title' tag
   ##
   } elsif ($html =~ m/[\s]*<a href=\"http:\/\/messages\.finance\.yahoo\.com\/profile\?bn=.*?\" target=\"_new\" title=\"(.*?)\" class=\"[\w\s]+\"[\s]*>.*?<\/a>/) {
      $data{"user"} = clean($1);
   }
   
   ##
   ## Body
   ##
   if ($html =~ m/<div class="user-data" style="padding-top: 5px;(?:\_height:[\d]+px;min\-height:[\d]+px;)">(.*?)<\/div>/s) {
      $data{"body"} = clean($1);
   }
   
   ##
   ## Sentiment
   ##
   if ($html =~ m/<b>[\n\s]*Sentiment[\s]*:[\n\s]*<\/b>[\n\s]*(.*?)[\n\s]*<\/div>/s) {
      $data{"sentiment"} = $1;
   }
   
   ##
   ## Message Date
   ##
   if ($html =~ m/<span (?:class|style)="[\w\;\-\:]+">[\s]*([\d]{1,2})\-([A-Z][a-z]{2,2})\-([\d]{2,2}) ([\d]{2,2}):([\d]{2,2}) ([amp]{2,2})<\/span>/) {
      # $1 -> day
      # $2 -> month
      # $3 -> year
      # $4 -> hour
      # $5 -> minute
      # $6 -> am/pm
      ($data{"msg_date"}, $timestamp) = convert_date(0, $5, $4, $6, $1, $2, $3);
   ##
   ## Sometimes we don't have a date, but the number of minutes ago the 
   ## message was posted. Well, we can handle that too!
   ##
   } elsif ($html =~ m/<span style=\"vertical-align:middle;\">([\d]{1,2}) minutes ago<\/span>/) {
      $timestamp = time() - ($1 * 60);
      my @t = localtime($timestamp);
      $data{'msg_date'} = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0]);
   ##
   ## And then sometimes we can get seconds!! Man we are quick!
   ##
   } elsif ($html =~ m/<span style=\"vertical-align:middle;\">([\d]+) second\(s\) ago<\/span>/) {
      $timestamp = time() - ($1 * 60 * 60);
      my @t = localtime($timestamp);
      $data{'msg_date'} = sprintf("%04d-%02d-%02d %02d:%02d:%02d", $t[5]+1900, $t[4]+1, $t[3], $t[2], $t[1], $t[0]);
   }
   #CollectorCommon::debug_dump(%data);
   
   ##
   ## We need to figure out who our parent is, if there is one
   ## It's kind of a hack, but we are basing it on pixel spacing information
   ## This could really change at any moment, so we should have the calling scrpt
   ## check if we aren't getting back any parent information
   ##
   my $our_width = undef;
   my $parent_url = undef;
   my @lines = split(/\n/, $html);
   my $flag = undef;
   for (my $ctr = $#lines; $ctr >= 0; $ctr--) {
      ##
      ## First find the thread that is highlighted
      ##
      if (!defined($flag) && $lines[$ctr] =~ /table-cell cell-highlight/) {
         $flag = $ctr;
         next;
      }
      next unless (defined($flag));
      ##
      ## Now look to figure where we are
      ## We then find the first posting that is less than ours. That's our parent!
      ##
      if (!defined($our_width) && $lines[$ctr] =~ m/style=\"padding: 5px 0 5px ([-\d]+)px;\" valign=\"top\">/) {
         $our_width = $1;
         next;
      }
      next unless (defined($our_width));
      if ($lines[$ctr] =~ m/<a class=\"syslink\" href=\"(http:\/\/messages\.finance\.yahoo\.com\/Stocks\_.*?)\"/) {
         $parent_url = $1;
      }
      ##
      ## Bingo!
      ##
      if ($lines[$ctr] =~ m/style=\"padding: 5px 0 5px ([-\d]+)px;\" valign=\"top\">/) {
         if (int($1) < int($our_width)) {
            $data{"parent_url"} = $parent_url;
            $data{"parent"}     = $CollectorCommon::PARENT_URL;
            last;
         }
      }
   } # FOR
   push(@ret, \%data);
   return ($timestamp, @ret);
}

## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   
   ##
   ## Check for symbol switches...
   ##
#    print "\$symbol: $symbol\n";
#    print "No longer: ".($html =~ /$symbol[\s]?is no longer valid/i)."\n";
#    print "Changed: ".($html =~ /It has changed to[\s]?<a href=".*?">([\w\d\.]+)<\/a>/i)."\n";
#    print $html;
   
   if ($html =~ m/$symbol[\s]?is no longer valid/i &&
       $html =~ m/It has changed to[\s]?<a href=".*?">([\w\d\.]+)<\/a>/i) {
      StockInfo::symbol_change($stock_id, $1);
      return (1);
   }
   
   return ($html =~ /There are no topics in this message board/ ||
           $html =~ /This message is hidden by your ratings filter/ ||
           $html =~ /Sorry, the resource you are trying to access does not exist/ ||
           $html =~ /This topic has been deleted/ ||
           $html =~ /Symbol Lookup from Yahoo/ ||
           $html =~ /<div class=".*?"><h1><\/h1><span>\(\: $symbol\)<\/span><\/div>/ ||
           $html =~ /You have specified a non-existent message board/ ||
           $html =~ /Error:(?:<\/b>)?[\s]*There are no messages under this topic yet/i ||
           $html =~ /Add $symbol Headlines to My Yahoo/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   ##
   ## There are two types of lists
   ## The first is just a link to get us to the REAL list of messages
   ## that we want. The other is this real list. See geturls for more info
   ## In either case, we just need to know whether the key phrase below is
   ## in the html
   ##
   return ($html =~ /List as Individual Messages/ ||
           $html =~ /There is no[\s]+data available for $symbol/);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   
   ## There is nothing for us
   return (@ret) if ($html =~ /There is no[\s]+data available for $symbol/);
   
   ##
   ## Grab the URL to the flat list of recent messages
   ##
   if ($html =~ m/<a href=\"(http\:\/\/messages\.finance\.yahoo\.com\/.*)\">List as Individual Messages<\/a>/) {
      push(@ret, $1);
   ##
   ## It's the flat list of messages, which is what we want
   ##
   } else {
      my @lines = split(/\n/, $html);
      for (my $idx = 0; $idx <= $#lines; $idx++) {
         ##
         ## Find when a message post starts
         ##
         if ($lines[$idx] =~ m/<span style=\"clear: none; float: left; margin: 0px; padding: 0px 2px\">/) {
            for (; $idx <= $#lines; $idx++) {
               ##
               ## Ok, so now we need ot find our URL
               ## Once we find what we are looking for, we can break out and look
               ## for another one
               ##
               if ($lines[$idx] =~ m/<a class=\"syslink\" href=\"(http:\/\/.*?)\"/) {
                  my $temp = $1;
                  if ($lines[$idx+1] =~ /[\s]*title=\".*?\">/) {
                     push(@ret, "$temp&off=1");
                     last;
                  } # IF
               } # IF
            } # FOR
         } # IF
      } # FOR
      ##
      ## Check whether there is a more data...
      ##
      if ($html =~ m/<a href="(http\:\/\/messages\.finance\.yahoo\.com\/.*)\"><span class=\"pagination\">Older \&gt;<\/span><\/a>/g) {
         push(@ret, $1);
      }
   }
   return (@ret);
}

1;
