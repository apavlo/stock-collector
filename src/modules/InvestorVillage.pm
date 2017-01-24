package InvestorVillage;

use strict;
use warnings;
use CollectorCommon qw(clean convert_date);

## ===========================================================
## Investor Hub
## ===========================================================

##
## Base URL
## 
my $base = "http://investorvillage.com/";

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $stock_id) = @_;
   my %data = ( );
   my $timestamp;
   
   ##
   ## Title + Body
   ##
   if ($html =~ m/<b><span[\s]+>[\s\n]+(.*?)<\/span><\/b><\/div>[\s]*(?:<div>)?(.*?)[\s\n]*(?:<\/div>)?[\s\n]*<br><br>[\s\n]*<\/div>/s) {
      $data{'title'} = clean($1);
      $data{'body'}  = clean($2);
   } else {
      CollectorCommon::shutdown("FAILED TO GET TITLE/BODY FOR INVESTORVILLAGE!");
   }
   ##
   ## User
   ##
   if ($html =~ m/<b>Author:\&nbsp\;<\/b>[\s]*<a[\s]*href=\"viewprofile\.asp\?m=[\d\w]+\" style=\".*?\">(.*?)<\/a>/) {
      $data{'user'} = clean($1);
   }
   ##
   ## Message Date
   ##
   if ($html =~ m/<b>Msg<\/b>: [\d]+ of [\d]+[\s]*\&nbsp\;[\s]*\&nbsp\;[\s]*([\d]{1,2})\/([\d]{1,2})\/([\d]{4,4}) ([\d]{1,2}):([\d]{2,2}):([\d]{2,2}) ([AMP]{2,2})/) {
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
   } elsif ($html =~ m/<b>Msg<\/b>: [\d]+ of [\d]+[\s]*\&nbsp\;[\s]*\&nbsp\;[\s]*([\d]{1,2})\/([\d]{1,2})\/([\d]{4,4})[\s]*\&nbsp\;[\s]*\&nbsp\;[\s]*/) {
      # $1 -> month
      # $2 -> day
      # $3 -> year
      # $4 -> hour
      # $5 -> minute
      # $6 -> second
      # $7 -> am/pm
      ($data{"msg_date"}, $timestamp) = convert_date(0, 0, 0, "am", $2, $1, $3);
   }
   
   ##
   ## Sentiment
   ##
   if ($html =~ m/<div style="float:right; padding: [\d]px 0 0 0;">\n<b>(.*?)<\/b>/s) {
      $data{'sentiment'} = clean($1) if (length($1) > 0);
   }
   ##
   ## Parent URL
   ##
   if ($html =~ m/In response to <a href="(smbd\.asp\?mb=[\d]+&mn=[\d]+&pt=msg&mid=[\d]+)">msg [\d]+<\/a> by/) {
      $data{"parent_url"} = $base.$1;
      $data{"parent"}     = $CollectorCommon::PARENT_URL;
   }
   
   my @ret = ( );
   push(@ret, \%data);
   #CollectorCommon::debug_dump(%data);
   return ($timestamp, @ret);
}

## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /There were no boards found\. As a registered member, you can request new boards/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /<form name="GoMessage[\d]" method="get">Go\&nbsp\;to\&nbsp\;Msg\#/ ||
           $html =~ /<a href=\"findboard\.asp\">Message Board Directory<\/a>/);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   while ($html =~ m/<td>[\d]+<\/td><td>[\s]*<a href="((?:smbd|groups)\.asp\?mb=[\d]+&mn=[\d]+&pt=msg&mid=[\d]+)">.*?<\/a><\/td>/g) {
      my $url = $base.$1;
      push(@ret, $url) unless (grep(/$url/, @ret));
   } # WHILE
   while ($html =~ m/<td class=\"cell_small\"><a href=\"((?:smbd|groups)\.asp\?mb=[\d]+&pt=m)\">$symbol<\/a><\/td>/g) {
      my $url = $base.$1;
      push(@ret, $url) unless (grep(/$url/, @ret));
   } # WHILE
   
   ##
   ## Figure out whether there are more messages to look for
   ##
   if ($html =~ m/ShowPrevious\(\)\{[\s]+document\.location='((?:smbd|groups)\.asp\?mb=[\d]+&pt=m&d=[-\d]+&r=&pm=[\d]+&nh=[\d]+)'\}/) {
      my $url = $base.$1;
      push(@ret, $url) unless (grep(/$url/, @ret));
   }
   return (@ret);
}

1;
