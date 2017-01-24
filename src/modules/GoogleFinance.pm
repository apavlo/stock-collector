package GoogleFinance;

use strict;
use warnings;
use CollectorCommon qw(clean convert_date);

## ===========================================================
## Google Finance Boards
## ===========================================================

##
## Base URL
## 
my $base_url = "http://finance.google.com";

## -----------------------------------------------------------
## parse
## -----------------------------------------------------------
sub parse {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   my $timestamp;
   
   my $first = 1;
   my %data = ( );
   my @lines = split(/\n/, $html);
   my $title = undef;
   for (my $ctr = 0; $ctr < $#lines; $ctr++) {
      ##
      ## User
      ## This will always come first for each message
      ##
      if ($lines[$ctr] =~ m/<b class="msghk">From:<\/b><\/nobr><\/td><td width="2"><\/td>/) {
         if ($lines[++$ctr] =~ m/<b><font class="inheritcolor" color="\#[\d\w]{6,6}">([\w\d\W]+)<a target=_parent href=".*?">...<\/a>\@(.*?)<\/font><\/b>/) {
            ##
            ## If the data table is undefined, then we know that this is the first time
            ## Otherwise we need to save the last array and set the parent id value
            ## to something that well tell the calling function to get it from the first insert
            ##
            if (defined($data{'user'})) {
               my %copy = ( );
               $copy{$_} = $data{$_} foreach (keys %data);
               push(@ret, \%copy);
               %data = ( );
               $data{"parent"} = $CollectorCommon::PARENT_FIRST;
               $data{"title"}  = "Re: $title";
            }
            $data{"user"} = "$1\@$2";
         }
      ##
      ## Date
      ##
      } elsif ($lines[$ctr] =~ m/<b class="msghk">Date:<\/b><\/nobr><\/td><td width="2"><\/td>/) {
         if ($lines[++$ctr] =~ m/<span class="msghv">[A-Z][a-z]{2,4}, ([A-Z][a-z]{2}) ([\d]{1,2}) ([\d]{4,4}) ([\d]+):([\d]+)&nbsp;([apm])/) {
            ($data{"msg_date"}, $timestamp) = convert_date(0, $5, $4, $6, $2, $1, $3);
         }
      ##
      ## Title
      ##
      } elsif ($lines[$ctr] =~ m/<td align=left width=100\%><span class="fontsize5"><b>(.*?)<\/b><\/span><\/td>/) {
         $title = clean($1);
         $data{'title'} = $title;
      ##
      ## Body
      ##
      } elsif ($lines[$ctr] =~ m/<a name="msg_[\w\d]+"><\/a>(.*?)$/) {
         my $body = $1;
         while ($lines[++$ctr] !~ /^[\s]+<\/div>$/) {
            $body .= "\n".$lines[$ctr];
         } # WHILE
         $data{"body"} = clean($body);
      }
   }
   ##
   ## This makes sure we pick up the last message
   ##
   if (defined($data{"user"}) && defined($data{"body"})) {
      my %copy = ( );
      $copy{$_} = $data{$_} foreach (keys %data);
      push(@ret, \%copy);
   }
#    for (my $ctr = 0; $ctr < $#ret; $ctr++) {
#       CollectorCommon::debug_dump(%{$ret[$ctr]});
#    }
#    CollectorCommon::shutdown();
   
   return ($timestamp, @ret);
}

## -----------------------------------------------------------
## notfound
## -------------------------------------------------------
sub notfound {
   my ($html, $symbol, $stock_id) = @_;
   return ($html =~ /Your search - <b>$symbol<\/b> - produced no matches/ ||
           $html =~ /This group has 0 discussions/);
}

## -----------------------------------------------------------
## islist
## -------------------------------------------------------
sub islist {
   my ($html, $symbol, $stock_id) = @_;
   return (($html =~ /<td>View by:&nbsp;/ && $html =~ /<nobr><b>topics<\/b><\/nobr>/) ||
           $html =~ /<a href="(.*?\/groups\/finance\?.*?)"[\s]*>More discussions.*?<\/a>/);
}

## -----------------------------------------------------------
## geturls
## -------------------------------------------------------
sub geturls {
   my ($html, $symbol, $stock_id) = @_;
   my @ret = ( );
   
   ##
   ## Grab the URL to the flat list of recent messages
   ## We get this from going to the source.base_url
   ##
   if ($html =~ m/<a href="(.*?\/groups\/finance\?.*?)"[\s]*>More discussions.*?<\/a>/) {
      push(@ret, $base_url.$1);
   ##
   ## It's the flat list of messages, which is what we want
   ##
   } else {
      while ($html =~ m/<a href="(\/group\/google\.finance\.[\d]+\/browse_thread\/thread\/[\w\d]+\?.*?\#)">(?:<font size="\+0">).*?(?:<\/font>)<\/a>/g) {
         push(@ret, $base_url.$1);
         #print "[1] $base_url.$1\n";
      } # WHILE
      ##
      ## Check whether there is a more data...
      ##
      if ($html =~ m/<a href="(\/group\/google\.finance\.[\d]+\?.*?start=[\d]+&sa=N)">Older[\s]+\&raquo;<\/a>/) {
         push(@ret, $base_url.$1);
         #print "[2] $base_url.$1\n";
      } # IF
   }
   return (@ret);
}

1;