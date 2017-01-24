package AllStocks;

use strict;
use warnings;

use CollectorCommon qw(clean convert_date);

## ===========================================================
## AllStocks.com
## ===========================================================

##
## The message boards we spider
##    2  => Hot Stocks Free for All
##    8  => Micro Penny Stocks, Penny Stocks $0.10 & Under
##    16 => .11 and Up!
##
my @GROUPS = ( 2, 8, 16 );

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
   my ($html, $symbol, $url) = @_;
   my $timestamp;

   my $title    = undef;
   my @user     = ( );
   my @body     = ( );
   my @msg_date = ( );

   ##
   ## Title
   ## Note that we only use the title to figure out what stock they are talking about
   ## Some of the child messages may divert to another stock, but we assume that
   ## that the discussion is always about the one the topic lists
   ##
   if ($html =~ m/This is topic <b>(.*?)<\/b> in forum/) {
      $title = clean($1);
      
      ##
      ## Find the stock
      ## Bail out if we can't get it
      ##
      $symbol = CollectorCommon::find_symbol($title) unless (defined($symbol));
      unless (defined($symbol)) {
         ##
         ## There wasn't a symbol that we knew about, so we need to make sure the calling script
         ## marks the url as "No Symbol Found". This is kind of screwy, but this is how
         ## it needs to be done
         ##
         my %data = ( "symbol" => undef );
         my @ret  = ( \%data );
         return (undef, @ret);
      } # UNLESS
   }
   
   ##
   ## Now for the rest of the page, there are multiple posts on a single page
   ##
   my @lines = split(/\n/, $html);
   my $idx = -1;
   for (my $ctr = 0; $ctr < $#lines; $ctr++) {
      ##
      ## User
      ##
      if ($lines[$ctr] =~ m/<hr[\s]*\/>Posted by <b>(.*?)<\/b>[\s]*on[\s]*/) {
         $user[++$idx] = $1;
         next;
      }
      ##
      ## Message Date
      ##
      if ($lines[$ctr] =~ m/<span class=\"dateformat-[\d]+\">([A-Z][a-z]+) ([\d]{2,2}), ([\d]{4,4})<\/span>[\s]*<span class="timeformat-[\d]+">([\d]{2,2}):([\d]{2,2})<\/span>/) {
         ##
         ## Something bad happened if we are getting a message date before the user
         ##
         if ($idx < 0 || defined($msg_date[$idx])) {
            my $subject = "ERROR: Unable to parse AllStocks.com";
            my $message = "Found a message date before we got the user for stock $symbol.\n$url";
            CollectorCommon::send_email($subject, $message);
            print debug_print("$subject - Sent email notice!");
            CollectorCommon::shutdown();
         }
         # $1 -> month
         # $2 -> day
         # $3 -> year
         # $4 -> hour
         # $5 -> minute
         ($msg_date[$idx], $timestamp) = convert_date(0, $5, $4, undef, $2, substr($1, 0, 3), $3);
         
         ##
         ## Next line is always the body
         ##
         $body[$idx] = clean($lines[$ctr+1]);
         next;
      }
   } # FOREACH
   ##
   ## Good to go
   ##
   my @ret = ( );
   for (my $ctr = 0; $ctr <= $idx; $ctr++) {
      my %data = ( "title"     => $title,
                   "symbol"    => $symbol,
                   "body"      => $body[$ctr],
                   "msg_date"  => $msg_date[$ctr],
                   "user"      => $user[$ctr],
      );
      push(@ret, \%data);
   }
   return ($timestamp, @ret);
}
         
## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Error: No forum selected/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Topic Starter/ ||
           $html =~ /Printer-friendly view of this topic/);
}

## -----------------------------------------------------------
## geturls
## -----------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   while ($html =~ m/<a href=\"(http:\/\/www\.allstocks\.com\/stockmessageboard\/ubb\/ultimatebb\.php\/ubb\/get_topic\/f\/[\d]+\/t\/[\d]+\.html)">.*?<\/a>/g) {
      push(@ret, $1) unless (grep(/$1/, @ret));
   } # WHILE
   ##
   ## Figure out whether there are more messages to look for
   ##
   while ($html =~ m/<a href=\"(http:\/\/www\.allstocks\.com\/stockmessageboard\/ubb\/ultimatebb\.php\/ubb\/get_topic\/f\/[\d]+\/start_point\/[\d]+\/hardset\/[\d]+.html)">[\d]+<\/a>/g) {
      #push(@ret, $1) unless (grep(/$1/, @ret));
   } # WHILE
   ##
   ## Printer friendly page has what we really want!
   ##
   if ($html =~ m/<a href=\"(http:\/\/www\.allstocks\.com\/stockmessageboard\/cgi-bin\/ultimatebb\.cgi\?ubb=print_topic;f=[\d]+;t=[\d]+)\"><img src=\".*?print_topic.gif\" border=\"[\d]\" alt=\".*?\" title=\"\"[\s]*\/>[\s]*Printer-friendly view of this topic<\/a>/) {
      push(@ret, $1) unless (grep(/$1/, @ret));
   }
   return (@ret);
}

1;