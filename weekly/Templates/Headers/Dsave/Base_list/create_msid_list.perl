#/usr/bin/perl 

#
#---- this extract web name of the table containing msids
#---- you need to give a full path such as
#---- perl create_msid_list.perl /data/mta4/www/REPORTS/2015/0115.html
#---- to extract ACIS data (for this specific example)
#
#    author: t. isobe (tisobe@cfa.harvard.edu)
#
#       last update Sep. 16, 2015
#
#

$input = $ARGV[0];

open(FH, "$input");
open(OUT, ">./zout");
$chk = 0;
while(<FH>){
    chomp $_;
    if ($_ =~ /mta\/DAILY\/mta_deriv\// && $_ =~ /\.html/){
        @atemp = split(/mta_deriv\//, $_);
        @btemp = split(/\.html/, $atemp[1]);
        print OUT  "\n\n$btemp[0]";
        print OUT '.html<>';
        $chk = 1

    }elsif($chk > 0 && $_ =~ /gif/){ 
        @atemp = split(/gif\'\)\"\>/, $_);
        @btemp = split(/\</, $atemp[1]);
        print OUT  "$btemp[0]";
        print OUT  ":";
    }
}
close(FH);
close(OUT);
