package RagingBull;

use strict;
use warnings;
use POSIX;
use CollectorCommon qw(clean convert_date);

## ===========================================================
## RAGING BULL
## ===========================================================

##
## Since technically these messages don't have titles, we are 
## going to use the titles from the message lists
##
my %titles = ( );

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $stock_id) = @_;  
   my $timestamp;
   my %data = ( );

   ##
   ## Message Date
   ##
   if ($html =~ /(\d\d) (\w+) (\d\d\d\d),\s?(\d+)\:(\d\d) ([AMP])/gs) {
      # $1 -> day
      # $2 -> month
      # $3 -> year
      # $4 -> hour
      # $5 -> minute
      # $6 -> am/pm
      ($data{"msg_date"}, $timestamp) = convert_date(0, $5, $4, $6, $1, $2, $3);
   }
   ##
   ## User
   ##
   if ($html =~ /By:[\s]+<a href=\".*?member=(.+)\">/) {
         $data{"user"} = clean($1) ;
   }
   ##
   ## Body
   ##
   if ($html =~ m/<span style=\"line-height: 1\.4;\" class=\"t\">(.*?)<br[\s]*\/>[\n\s]+<\/span>/s) {
      my $body = clean($1);
      ##
      ## Sentiment
      ##
      if ($body =~ m/ST Rating\- <strong>(.*?)<\/strong>/) {
         $data{"sentiment"} = $1 ;
      }
      $body =~ s/<EM><font size=-1>\(Voluntary Disclosure: .*?\)<\/EM>.*//i;
      chomp($body);
      $data{"body"} = $body;
   }
   ##
   ## Title
   ## Pull the message id from the post and see if we have a title in our cache
   ##
   if ($html =~ m/Msg.[\s]+([\d]+)[\s]+of[\s]+/) {
      my $msg_id = int($1);
      print "\$msg_id -> $msg_id\n";
      if (defined($titles{$msg_id})) {
         $data{"title"} = $titles{$msg_id} ;
         delete $titles{$msg_id};
      ##
      ## It's not in our cache, so we'll cheat and just use the first 50 chars of the body before
      ## the first newline, which what they're really doing in the first place...
      ##
      } elsif (defined($data{"body"}) && length($data{"body"}) > 0) {
         $data{"title"} = substr((split(/\n/, $data{"body"}))[0], 0, 50);
         chomp($data{"title"});
      }
   }
   ##
   ## Pull out the parent url if there is one
   ## The calling script is responsible for figuring out what the id for it is
   ##
   if ($html =~ m/\(This msg\. is a reply to <a href="(.*?)"[\s]+>[\d]+<\/a>[\s\n]+by/s) {
      $data{"parent"} = $CollectorCommon::PARENT_URL;
      $data{"parent_url"} = "http://ragingbull.quote.com$1";
   } 
   
   #CollectorCommon::debug_dump(%data);
   #CollectorCommon::shutdown();
   my @ret = ( \%data );
   return ($timestamp, @ret);
}

## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Sorry, no board was found for $symbol/ ||
           $html =~ /No Posts on this board.[\s]+<MB postlink><strong>.*? Post new message now!<\/strong>/ ||
           $html =~ /Oops\! That message number is invalid on this board/ ||
           $html =~ /That message number is not found on this board/ ||
           $html =~ /<b>oops, sorry<\/b>/i ||
           $html =~ /Error[\s]+-[\s]+There was a problem\./i ||  
           $html =~ /Oops\! There was a problem\./);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Refresh (This Page|list)/i && $html =~ /Post new message/i);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   my $base = "http://ragingbull.quote.com";
   
   while ($html =~ m/(\/mboard\/boards\.cgi\?board=$symbol&read=[\d]+)\">(.*?)<\/a>/g) {
      my $add_url = $1;
      my $title   = CollectorCommon::clean($2);
      next if ($add_url =~ /&read=0$/);
      ##
      ## Grab the message id and add it to the our title cache
      ##
      if ($add_url =~ m/board=$symbol&read=([\d]+)/) {
         $titles{$1} = $title;
         #print "$1 -> $title\n";
      }
      push(@ret, $base.$add_url);
      #print "$base.$add_url\n";
   } # WHILE
   ##
   ## Figure out whether there are more messages to look for
   ##
   if ($html =~ m/<a href="(\/mboard\/boards\.cgi\?board=$symbol.*?&endat=[\d])" accesskey="z">/) {
      ##
      ## We also need to figure out what the TODO: ????
      ##
      my $add_url = $base.$1;
      push(@ret, $add_url);
      #print "$add_url\n";
   }
   return (@ret);
}

1;
