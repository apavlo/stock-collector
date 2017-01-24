package TheLion;

use strict;
use warnings;
use CollectorCommon qw(clean convert_date);

## ===========================================================
## Investor Hub
## ===========================================================

##
## Base URL
## 
my $base = "http://www.thelion.com";

##
## IMPORTANT!
## They make a distinction between stock symbols with and without the
## exchange suffix. So XXX is different than XXX.OB. This sucks
## but we can easily deal with it. Make sure that when there is a 
## dot in the symbol you convert it to an underscore when you
## are fishing around in the html
##

##
## TheLion.com has stock-specific message boards as well as general message
## boards. The following URL will list all the message boards, which allow
## us the rape & pillage them as needed 
##
my $ALL_FORUMS_URL = "$base/bin/forum.cgi?cmd=list_all";

##
## Since technically these messages don't have titles, we are 
## going to use the titles from the message lists
##
my %titles = ( );
my %sentiments = ( );
my %symbols = ( );

my %parent_url_cache = ( );

## -----------------------------------------------------------
## groups
## ----------------------------------------------------------
sub groups {
   my @ret = ( );
   ##
   ## Disabling for now because there would be duplicate messages
   ##
   return (0, @ret); 
   ##
   ## Go and find all the groups from the forums list
   ##
   my $html = CollectorCommon::wget_get_url($ALL_FORUMS_URL);
   while ($html =~ m/<a href=(\/bin\/forum\.cgi\?tf=[\w\d\_]+)>.*?<\/a>/g) {
      my $url = $base.$1;
      push(@ret, $url) unless (grep(/$url/, @ret));
   } # WHILE
   #@ret = ( $base."/bin/forum.cgi?tf=the_itt_jacuzzi" );
   return (0, @ret);
}

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $url) = @_;
   my %data = ( );
   my $timestamp;

   ##
   ## Title
   ## Pull the title from our cache if we can get the message id
   ##
   if ($html =~ m/<a href=\/bin\/forum\.cgi\?.*?&cmd=reply&msg=([\d]+).*?>(?:<span class=a_bold>)?Reply(?:<\/span>)?<\/a>/) {
      my $msg_id = $1;
      if (defined($titles{$msg_id})) {
         $data{'title'} = $titles{$msg_id};
         $data{'sentiment'} = $sentiments{$msg_id};
         
         ##
         ## Remove 'eom' from the title
         ##
         $data{'title'} =~ s/[\s]+eom$//;
         $data{'title'} = clean($data{'title'});
         ##
         ## Symbol Lookup
         ##
         unless (defined($symbol)) {
            $data{'symbol'} = (defined($symbols{$msg_id}) ? $symbols{$msg_id} : CollectorCommon::find_symbol($data{'title'}));
         } # UNLESS
         delete $symbols{$msg_id} if (defined($symbols{$msg_id}));
         delete $titles{$msg_id};
         delete $sentiments{$msg_id};
      }
   }
   ##
   ## User
   ##
   if ($html =~ m/From:[\s]+(?:<span class=.*?>)?<a href=\/bin\/(?:show)?profile\.cgi\?c(?:md)?=s(?:how)?&ru_name=.*?>(.*?)<\/a>(?:<\/span>)?/) {
      $data{'user'} = clean(CollectorCommon::strip_html($1));
   }
   ##
   ## Body
   ## I think they have crappy code and forget the span tag when there is no bio
   ##
   if ($html =~ m/<tr><td colspan=3><table><tr><td>(?:<span class=a[\d]+>)?[\n]*(.*?)(?:<\/span>)?(?:<br>|\n)*(?:<table style='.*?'><tr><td style='.*?'><img src=.*?\/quote\.gif>[\s]+Reply|<\/td><\/tr><\/table><\/td><\/tr><tr class=.*?><td id="bottom_menu_l"|<\/td>(?:<\/tr><\/table><\/td>)?<td id="yahoo_chart.*?">)/i) {
      $data{'body'} = $1;
      ## Remove facebook/digg clutter
      $data{'body'} =~ s/<p class=rb>Share This Post:[\n]*.*//si;
      $data{'body'} = clean($data{'body'});
   }
   ##
   ## Message Date
   ## Example: 06/29/2009 13:09
   ##
   if ($html =~ m/Date:[\s]+([\d]{2,2})\/([\d]{2,2})\/([\d]{4,4}) ([\d]{2,2}):([\d]{2,2})/) {
      # $1 -> month
      # $2 -> day
      # $3 -> year
      # $4 -> hour
      # $5 -> minute
      # $6 -> second
      ($data{'msg_date'}, $timestamp) = convert_date(0, $5, $4, undef, $2, $1, $3);
   }
   ##
   ## Parent URL
   ## This is difficult because they switch between stock specific boards and "global boards"
   ##
   #if ($html =~ m/<td[\s]*(?:style='.*?')?>(?:<img src=.*?quote\.gif>)?[\s]*Reply to .*?[\s]+-[\s]+Msg \#<a href=(\/bin\/forum\.cgi\?.*?&msg=[\d]+&cmd=r(?:ead)?.*?)>[\d]+<\/a>/) {
   if ($html =~ m/<table id=\"reply_to_t\">.*?<img src=.*?quote\.gif>[\s]*Reply to <a href=.*>(.*?)<\/a>[\s]+-[\s]+Msg \#<a href=(\/bin\/forum\.cgi\?tf=.*?&msg=[\d]+)&t=0&cmd=r(?:ead)?.*?>[\d]+<\/a>[\s]+-[\s]+([\d]{2,2})\/([\d]{2,2})\/([\d]{4,4}) ([\d]{2,2}):([\d]{2,2})(?:<br>)+(.*?)<\/td>/i) {
      # $3 -> month
      # $4 -> day
      # $5 -> year
      # $6 -> hour
      # $7 -> minute
      # $8 -> second
      my ($parent_date, undef) = convert_date(0, $7, $6, undef, $4, $3, $5);
      
      my $key = CollectorCommon::clean($1)."|$parent_date";
      if (defined($parent_url_cache{$key})) {
#          print CollectorCommon::debug_print("Found parent url in cache for '$key'");
         $data{"parent_url"} = $parent_url_cache{$key};
         $data{"parent"}     = $CollectorCommon::PARENT_URL;
      }
   }
   
   my @ret = ( );
   push(@ret, \%data);
#    if ($data{"parent_url"}) {
#       CollectorCommon::debug_dump(%data);
#       CollectorCommon::shutdown();
#    }
   return ($timestamp, @ret);
}

## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /The (symbol|trader forum|stock forum) you have (chosen|selected) ['\"]?<b>.*?<\/b>['\"]? does not exist or is no longer available/ ||
           $html =~ /There (is|are) no messages in this forum/ ||
           $html =~ /Message #[\d]+ does not exist or is no longer available\./ ||
           $html =~ /<td id=\".*?\">Forum - .*?<\/td><td id=\".*?\">0 message<\/td>/ ||
           (defined($symbol) && $html =~ /\($symbol\) does not exist or is no longer available\./)||
           $html =~ /<li class=red>Do you mean to go to symbol [\w\.]+/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;

   ## Some symbols have the dot converted??
   ## WSPTQ.OB -> WSPTQ_OB
   $symbol .= "(?:_OB|_PK)?" if (defined($symbol));
   my $flag = get_url_flag($symbol);
#    print "flag -> $flag\n";
   
   return ($html =~ /<a href=\/bin\/forum\.cgi\?$flag(?:&t=|)(?:[\s]+style=".*?"|)>Refresh<\/a>/i ||
           $html =~ /<a href=\/bin\/forum\.cgi\?$flag(?:&t=|)(?:[\s]+style=".*?"|)>Top<\/a>/i);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   ##
   ## The URLs are just about the same between stock message boards
   ## and general discussion boards
   ##
   my $flag = get_url_flag($symbol);
   #    while ($html =~ m/<TD class=a8>([\d]+)<\/TD>(?:<TD.*?><a href=.*?finance\.yahoo\.com\/q\?s=[\w]+>([\w]+)<\/a><\/TD>)?<TD class=["]?(?:a9|tdwrap)["]?>.*?<A HREF=(\/bin\/forum\.cgi\?$flag&msg=[\d]+&cmd=r(?:ead)?)(?:&t=|)>(.*?)<\/a><\/TD><TD>([\w\s]*)<\/TD><TD class="[\w]+">.*?<\/TD><TD><a href=.*?>(.*?)<\/a><\/TD><TD class="[\w]+">(.*?)<\/TD>/gi) {
   while ($html =~ m/<TD class=a8>([\d]+)<\/TD>.*?<TD class=["]?(?:a9|tdwrap)["]?>.*?<A HREF=(\/bin\/forum\.cgi\?$flag&msg=[\d]+&cmd=r(?:ead)?)(?:&t=|)>(.*?)<\/a><\/TD><TD>([\w\s]*)<\/TD><TD class="[\w]+">.*?<\/TD><TD><a href=.*?>(.*?)<\/a><\/TD><TD class="[\w]+">(.*?)<\/TD>/gi) {
      my $url = $base.$2;
#       $symbols{$1} = CollectorCommon::clean($2);
      $titles{$1} = CollectorCommon::clean($3);
      $sentiments{$1} = CollectorCommon::clean($4);
      push(@ret, $url) unless (grep(/$url/, @ret));
      
#       print "$url\n";
      
      # Parent URL Caching
      my $parent_user = CollectorCommon::clean($5);
      if ($6 =~ m/([\d]{2,2})\/([\d]{2,2})\/([\d]{4,4}) ([\d]{2,2}):([\d]{2,2})/i) {
         # $1 -> month
         # $2 -> day
         # $3 -> year
         # $4 -> hour
         # $5 -> minute
         # $6 -> second
         my ($parent_date, undef) = convert_date(0, $5, $4, undef, $2, $1, $3);
         
         ## Terrible hack...
         my $key = "$parent_user|$parent_date";
         $parent_url_cache{$key} = $url;
#          print "$key -> $url\n";
      }
   } # WHILE
   ##
   ## Figure out whether there are more messages to look for
   ##
   if ($html =~ m/<a href=(\/bin\/forum\.cgi\?$flag&msg=[\d]+)(?:&t=|)(?:[\s]+style=".*?"|)>< Prev 25<\/a>[\s]+\|/) {
      push(@ret, $base.$1);
   }
   return (@ret);
}

sub get_url_flag {
   my ($symbol) = @_;
   ##
   ## The URLs are just about the same between stock message boards
   ## and general discussion boards
   ##
   my $flag = undef;
   if (defined($symbol)) {
      #$symbol =~ s/\./_/;
      $flag = "sf=$symbol";
   } else {
      $flag = "tf=[\\d\\w_\\.]+";
   }
   return ($flag);
}

1;
