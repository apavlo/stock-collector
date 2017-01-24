package YahooGroups;

use strict;
use warnings;

use CollectorCommon qw(clean convert_date);

## ===========================================================
## Yahoo! Groups
## ===========================================================

##
## Yahoo Account Information
##
our $COLLECTOR_USER = "**CHANGE ME***";
our $COLLECTOR_PASS = "**CHANGE ME***";

##
## The YahooGroups we spider
##
my @GROUPS = ( "PennyStockWatchman",
               "stockanalysts",
               "pennypicks",
               "freepennystockpicks",
               "pennystocktrader",
               "adaytradersdream",
               "pennybustershotpennypicks",
);

## -----------------------------------------------------------
## groups
## ----------------------------------------------------------
sub groups {
   return (1, @GROUPS);
}

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $stock_id) = @_;
   my %data = ( );
   my $timestamp;
   $data{'symbol'} = $symbol if (defined($symbol));

   ##
   ## Title
   ##
   if ($html =~ m/<td class=\"ygrp-topic-title\" align=left>(.*?)<\/td>/) {
      $data{'title'} = clean($1);
      return if ($data{'title'} =~ /history\.html/);
   }
   ##
   ## User
   ##
   if ($html =~ m/<a href=\"http:\/\/profiles\.yahoo\.com\/(.*?)\" title=\".*?\">.*?<\/a>/) {
      $data{'user'} = clean($1);
   } elsif ($html =~ m/<a href=\"http:\/\/profiles\.yahoo\.com\/(.*?)\">.*?<\/a>/) {
      $data{'user'} = clean($1);
   }
   ##
   ## Body
   ##
   if ($html =~ m/<div class=\"msgarea\">(.*?)<\/div>/s) {
      my $body = $1;
      ##
      ## There might be a bunch of Yahoo email crap we want to remove
      ##
      $body =~ s/__________________________________________________.*$//s;
      $body =~ s/<hr size="1">.$//s;
      $body =~ s/Get stock pick details of trigger price.*$//s;
      $data{'body'} = clean($body);
   }
   ##
   ## Message Date
   ##
   if ($html =~ m/[A-Z][a-z]{2,2} ([\w]{3,3})&nbsp;([\d]{1,2}),&nbsp;([\d]{4,4}) ([\d]{1,2}):([\d]{2,2})&nbsp;([amp]{2,2})/) {
      # $1 -> month
      # $2 -> day
      # $3 -> year
      # $4 -> hour
      # $5 -> minute
      # $6 -> am/pm
      ($data{"msg_date"}, $timestamp) = convert_date(0, $5, $4, $6, $2, $1, $3);
   }
   
   ##
   ## Important!
   ## Yahoo Groups aren't sorted by symbol
   ## So we need to figure out what stock this message is talking about
   ##
   if ($html =~ m/([\w]+) current price/) {
      ##
      ## Double check
      ##
      my $temp = $1;
      if ($data{'title'} =~ m/$temp \- [\d]+\% Potential Profit/) {
         $data{'symbol'} = $temp;
      }
   }
   ##
   ## At this point it gets a little trickier
   ## We need to go through EVERY stock symbol that we know about, and take the first
   ## and assume that this post is talking about that stock
   ##
   if (!defined($data{'symbol'})) {
      my $match;
      foreach my $text ($data{'title'}, $data{'body'}) {
         next unless (defined($text));
         $match = CollectorCommon::find_symbol($text);
         last if (defined($match));
      } # FOREACH
      $data{'symbol'} = $match;
   } # IF
   
   my @ret = ( \%data );
   return ($timestamp, @ret);
}
         
## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   
   ##
   ## Important!
   ## Send an email if we were logged out!
   ##
   my ($subject, $body);
   if ($html =~ /Login Form/) {
      $subject = "ERROR: Requires a Login For Group ".(defined($symbol) ? $symbol : "???");
      $body    = "Unable to spider Yahoo! Groups because the cookie expired!";
   ##
   ## Or our spidering account is not a member of the group
   ##
   } elsif ($html =~ /you need to enable Web Access/) {
      $subject = "ERROR: Not a Member of Group $symbol";
      $body    = "Unable to spider Yahoo! Groups for group $symbol because the ".
                 "account is not a member!";
   ##
   ## We overdid it!!
   ##
   } elsif ($html =~ /Unable to process request at this time/) {
      $subject = "ERROR: Got blocked by Yahoo!";
      $body = "Too much activity on Yahoo! caused them to block us. Bummer!";
   }
   
   if (defined($subject) && defined($body)) {
      CollectorCommon::send_email($subject, $body);
      print CollectorCommon::debug_print("ERROR: $subject - Sent email notice!");
      CollectorCommon::shutdown();
   }
   return ($html =~ /<h3>Group Not Found<\/h3>/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Group by Topic<\/a>/);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   my $base = "http://finance.groups.yahoo.com";
   while ($html =~ m/<a href=\"(\/group\/[\w\d]+\/message\/[\d]+)\?l=[\d]+"><span>(.*?)<\/span><\/a>/g) {
      my $url = $base.$1;
      push(@ret, $url) unless (grep(/$url/, @ret));
   } # WHILE
   ##
   ## Figure out whether there are more messages to look for
   ##
   if ($html =~ m/<a href="(\/group\/[\w\d]+\/messages\/[\d]+\?tidx=[\d]+)">Older&nbsp;&gt;<\/a>/) {
      push(@ret, $base.$1);
   }
   return (@ret);
}

1;