package InvestorHub;

use strict;
use warnings;
use CollectorCommon qw(clean convert_date);

## ===========================================================
## Investor Hub
## ===========================================================

##
## Since technically these messages don't have titles, we are 
## going to use the titles from the message lists
##
my %titles = ( );

##
## Base URL
## 
my $base = "http://investorshub.advfn.com/boards/";

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $stock_id) = @_;
   my %data = ( );
   my $timestamp;
   
   ##
   ## Title
   ## Pull the title from our cache if we can get the message id
   ##
   if ($html =~ m/Post \#([\d]+)[\s\n\t]+<\/title>/s ||
       $html =~ m/Post \#.*?<input .*? value="([\d]+)" .*>/i) {
      if (defined($titles{$1})) {
         $data{'title'} = $titles{$1};
         delete $titles{$1};
      }
   }
   ##
   ## User
   ##
   if ($html =~ m/<b>Posted by\:[\s]*<\/b>[\n\s\t]+<[aA] id=".*?" href=(?:'|")profile\.asp[x]?\?[uU]ser=[\d]+(?:'|")>(.*?)<\/a>/) {
      $data{'user'} = clean($1);
   }
   ##
   ## Body
   ## I think they have crappy code and forget the span tag when there is no bio
   ##
   if ($html =~ m/<div class='KonaBody'>(?:<span>)?(.*?)(?:<\/span>)?<\/div>/s) {
      $data{'body'} = clean($1);
   } elsif ($html =~ m/<span id=(?:'|")intelliTXT(?:'|")>(.*?)<\/span>/s) {
      $data{'body'} = clean($1);
   } elsif ($html =~ m/<span id=(?:'|")intelliTXT(?:'|")>(.*?)<\/td><\/tr><\/td><\/tr><\/table>/s) {
      $data{'body'} = clean($1);
   }
   
   ##
   ## Message Date
   ## Example: 'Thursday, May 29, 2008 10:03:02 PM'
   ##
   if ($html =~ m/<b>Date\:[\s]*<\/b>[\n\s\t]+<span id=".*?">[A-Z][a-z]+,[\s]+([A-Z][a-z]+)[\s]+([\d]{2}),[\s]+([\d]{4})[\s]+([\d]{1,2}):([\d]{2}):([\d]{2})[\s]+([AMP]{2})/s) {
      # $1 -> month
      # $2 -> day
      # $3 -> year
      # $4 -> hour
      # $5 -> minute
      # $6 -> second
      # $7 -> am/pm
      ($data{"msg_date"}, $timestamp) = convert_date($6, $5, $4, $7, $2, $1, $3);
   ##
   ## Message with no timestamp???
   ##
   }
#    } elsif ($html =~ m/<b>Date:<\/b>([\d]{1,2})\/([\d]{1,2})\/([\d]{4,4})<br>/) {
#       # $1 -> month
#       # $2 -> day
#       # $3 -> year
#       # $4 -> hour
#       # $5 -> minute
#       # $6 -> second
#       # $7 -> am/pm
#       ($data{"msg_date"}, $timestamp) = convert_date(0, 0, 0, "am", $2, $1, $3);
#    }
   
   ##
   ## Parent URL
   ##
   if ($html =~ m/<b>In reply to:[\s]+<\/b>[\n\s\t]+<[aA] id=".*?" href=(?:'|")profile\.asp[x]?\?[uU]ser=[\d]+(?:'|")>.*?[\s]*<\/a>[\s]*<span id=".*?">[\s]+who wrote[\s]+<\/span>[\s]*<[aA] id=".*?" href=(?:'|")(read_msg\.asp[x]?\?message_id=[\d]+)(?:'|")>msg\#[\s]+[\d]+<\/a>/) {
      $data{"parent_url"} = $base.$1;
      $data{"parent"}     = $CollectorCommon::PARENT_URL;
   }
   
#    CollectorCommon::debug_dump(%data);
   
   my @ret = ( );
   push(@ret, \%data);
   return ($timestamp, @ret);
}

## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /The board you requested was not found/ ||
           $html =~ /the board you requested was not found on InvestorsHub/ ||
           $html =~ /Message [\d]+ is deleted/ ||
           $html =~ /No Boards Were Found/ ||
           $html =~ /No Messages\.[\s]+<A href=\'.*?\'>Post the First Message\!<\/a>/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Next 50/ && $html =~ /Previous 50/ ||
           $html =~ /<th.*?><a href=".*?" style=".*?">Last Post<\/a><\/th>/ ||
           $html =~ /<title>Investors Hub - Search Results<\/title>/ ||
           $html =~ /<span style=(?:'|").*?(?:'|")>InvestorsHub<\/span><\/td><td colspan=[\d]+><span style=(?:'|").*?(?:'|")>SiliconInvestor<\/span>/);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   
   ##
   ## Message Posts
   ##
   while ($html =~ m/<td .*?nowrap[;]?">[\n\s\t]+<span id=".*?">\#([\d]+)<\/span>[\n\s\t]+&nbsp;[\n\s\t]+<\/td><td>[\n\s\t]+<[aA] id=".*?" href=(?:"|')(read_msg\.asp[x]?\?message_id=[\d]+)(?:"|')>(.*?)<\/a>[\n\s\t]+<\/td>/sg) {
      my $url = $base.$2;
      $titles{$1} = CollectorCommon::clean($3);
      #print "$1 $url -> $titles{$1}\n";
      push(@ret, $url) unless (grep(/$url/, @ret));
   } # WHILE
   
   ##
   ## Sometimes after the symbol lookup, we get a page with multiple boards.
   ## We'll take them all, muahahah!
   ##
   while ($html =~ m/<[aA] (?:class|id)=".*?" href=["']((?:\/boards\/)?board\.asp[x]?\?board\_id=[\d]+)["']>.*?(?:&nbsp;|\s)?\([\w\s]*$symbol[\w\s]*\)<\/a>/g) {
      my $url = $base.$1;
      $url =~ s/(\/boards){2,}/$1/;
      push(@ret, $url) unless (grep(/$url/, @ret));
   } # WHILE
   
   ##
   ## Figure out whether there are more messages to look for
   ##
   if ($html =~ m/<[aA] id=".*?" (?:class|id)=".*?" href=(?:"|')(board\.asp[x]?\?board_id=[\d]+&(?:amp;)(?:Prev|Next)Start=[\d]+)(?:"|')>Previous [\d]+<\/a>/) {
      my $url = $1;
      $url =~ s/&amp;/&/g;
      push(@ret, $base.$url);
   }
   return (@ret);
}

1;