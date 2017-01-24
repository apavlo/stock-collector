package HotPennyStocks;

use strict;
use warnings;

use CollectorCommon qw(clean convert_date);

## ===========================================================
## AllStocks.com
## ===========================================================

##
## The message boards we spider
##
my @GROUPS = ( "1.0", "6.0" );

my %FOUND_URLS = ( );

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
   my @ret = ();
   
   ##
   ## They made it so easy!
   ##
   while ($html =~ m/<hr size="[\d]+" width="100%" \/>[\s\t\n]*:[\s\t]+<b>(.*?)<\/b><br[\s]+\/>[\s\t\n]*:[\s\t]+<b>(.*?)<\/b>[\s]+<b>([A-Z][a-z]+) ([\d]{2,2}), ([\d]{4,4}), ([\d]{2,2}):([\d]{2,2}):([\d]{2,2}) ([APM]{2,2})<\/b>[\s\t\n]*<hr[\s]+\/>[\s\t\n]*<div style="margin: 0 [\d]+ex;">(.*?)<\/div>\n/gs) {
      my %data = ( );
      $data{'title'} = clean($1);
      $data{'user'}  = $2;
      $data{'body'}  = clean($10);
      # $3 -> month
      # $4 -> day
      # $5 -> year
      # $6 -> hour
      # $7 -> minute
      # $8 -> second
      # $9 -> am/pm
      ($data{'msg_date'}, $timestamp) = convert_date($8, $7, $6, $9, $4, substr($3, 0, 3), $5);
      
      ##
      ## Get the symbol if we don't have one yet
      ##
      $symbol = CollectorCommon::find_symbol($data{'title'}) unless (defined($symbol));
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
      $data{'symbol'} = $symbol;
      push(@ret, \%data);
   } # WHILE
   return ($timestamp, @ret);
}
         
## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /The topic or board you are looking for appears to be either missing or off limits to you/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Very Hot Topic \(More than 25 replies\)/ ||
           $html =~ /action=printpage\;topic=[\d\.]+/);
}

## -----------------------------------------------------------
## geturls
## -----------------------------------------------------------
sub geturls {
   my ($html, $symbol, $current_url) = @_;
   
   ##
   ## Always add the current url to our list so that we don't throw an
   ## error if we try to visit it twice
   ##
   $FOUND_URLS{$current_url} = 1;
   
   
   ##
   ## We will queue up the urls that we find in a temp array
   ## We need to clean them and remove the session id in it before we check whether
   ## we have already visited them before
   ##
   my @ret = ( );
   my @temp = ( );
   
   while ($html =~ m/<span id="msg_[\d]+"><a href="(http:.*?\/stockboard\/index\.php\?(PHPSESSID=[\d\w]+&amp;)?topic=[\d]+\.[\d]+)">.*?<\/a><\/span>/g) {
      push(@temp, $1) unless (grep(/$1/, @temp));
   } # WHILE
   ##
   ## Figure out whether there are more messages to look for
   ##
   while ($html =~ m/<a class="navPages" href="(http.*?\/stockboard\/index\.php\?(PHPSESSID=[\d\w]+&amp;)?board=[\d\.]+)">[\d]+<\/a>/g) {
      push(@temp, $1) unless (grep(/$1/, @temp));
   } # WHILE
   ##
   ## Printer friendly page has what we really want!
   ##
   if ($html =~ m/<a href="(http.*?\/stockboard\/index\.php\?(PHPSESSID=[\d\w]+&amp;)?action=printpage\;topic=[\d\.]+)" target="_blank">Print<\/a>/) {
      push(@temp, $1) unless (grep(/$1/, @temp));
   }
   ##
   ## Important!
   ## Strip out the session tag
   ##
   foreach my $url (@temp) {
      $url =~ s/PHPSESSID=[\d\w]+&amp;//;
      unless (defined($FOUND_URLS{$url})) {
         push(@ret, $url);
         $FOUND_URLS{$url} = 1;
      }
   } # FOR
   #print "FOUND:\n".join("\n", @FOUND_URLS)."\n";
   return (@ret);
}

1;