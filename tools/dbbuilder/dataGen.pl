#!/usr/bin/perl -w
print STDOUT "Enter the data size you want(Giga): ";
$size=<STDIN>;
unlink "region.tbl", "nation.tbl","supplier.tbl", "part.tbl","customer.tbl", "partsupp.tbl", "orders.tbl", "lineitem.tbl" ;
print STDOUT "Generating data sets ...\n";
system "./dbgen -s $size";
print STDOUT "Converting to Calpont format ...\n";
open(REGION, "<region.tbl") or die "Can't open region.tbl";
open(REGION1, ">region1.tbl") or die "Can't open region1.tbl";
while ( <REGION> )
{	
	chomp;
	(@token) = split /\|/ ;
	print REGION1 $token[0], ", '", $token[1], "', '", $token[2], "'\n";
}
close(REGION);
close(REGION1);
unlink "region.tbl";
rename "region1.tbl", "region.tbl";
open(NATION, "<nation.tbl") or die "Can't open region.tbl";
open(NATION1, ">nation1.tbl") or die "Can't open region1.tbl";
while ( <NATION> )
{	
	chomp;
	(@token) = split /\|/ ;
	print NATION1 $token[0], ", '", $token[1], "', ", $token[2], ", '", $token[3],"'\n";
}
close(NATION);
close(NATION1);
unlink "nation.tbl";
rename "nation1.tbl", "nation.tbl";
open(SUPPLIER, "<supplier.tbl") or die "Can't open region.tbl";
open(SUPPLIER1, ">supplier1.tbl") or die "Can't open region1.tbl";
while ( <SUPPLIER> )
{	
	chomp;
	(@token) = split /\|/ ;
	print SUPPLIER1 $token[0], ", '", $token[1], "', '", $token[2], "', ", $token[3], ", '", $token[4], "', ", $token[5], ", '", $token[6],"'\n";
}
close(SUPPLIER);
close(SUPPLIER);
unlink "supplier.tbl";
rename "supplier1.tbl", "supplier.tbl";
open(PART, "<part.tbl") or die "Can't open region.tbl";
open(PART1, ">part1.tbl") or die "Can't open region1.tbl";
while ( <PART> )
{	
	chomp;
	(@token) = split /\|/ ;
	print PART1 $token[0], ", '", $token[1], "', '", $token[2], "', '", $token[3], "', '", $token[4], "', ", $token[5], ", '", $token[6], "', ", $token[7], ", '", $token[8],"'\n";
}
close(PART);
close(PART1);
unlink "part.tbl";
rename "part1.tbl", "part.tbl";
open(CUSTOMER, "<customer.tbl") or die "Can't open region.tbl";
open(CUSTOMER1, ">customer1.tbl") or die "Can't open region1.tbl";
while ( <CUSTOMER> )
{	
	chomp;
	(@token) = split /\|/ ;
	print CUSTOMER1 $token[0], ", '", $token[1], "', '", $token[2], "', ", $token[3], ", '", $token[4], "', ", $token[5], ", '", $token[6], "', '", $token[7],"'\n";
}
close(CUSTOMER);
close(CUSTOMER1);
unlink "customer.tbl";
rename "customer1.tbl", "customer.tbl";
open(PARTSUPP, "<partsupp.tbl") or die "Can't open region.tbl";
open(PARTSUPP1, ">partsupp1.tbl") or die "Can't open region1.tbl";
while ( <PARTSUPP> )
{	
	chomp;
	(@token) = split /\|/ ;
	print PARTSUPP1 $token[0], ", ", $token[1], ", ", $token[2], ", ", $token[3], ", '", $token[4],"'\n";
}
close(PARTSUPP);
close(PARTSUPP1);
unlink "partsupp.tbl";
rename "partsupp1.tbl", "partsupp.tbl";
open(ORDERS, "<orders.tbl") or die "Can't open region.tbl";
open(ORDERS1, ">orders1.tbl") or die "Can't open region1.tbl";
while ( <ORDERS> )
{	
	chomp;
	(@token) = split /\|/ ;
	print ORDERS1 $token[0], ", ", $token[1], ", '", $token[2], "', ", $token[3], ", '", $token[4], "', '", $token[5], "', '", $token[6], "', ", $token[7], ", '", $token[8],"'\n";
}
close(ORDERS);
close(ORDERS1);
unlink "orders.tbl";
rename "orders1.tbl", "orders.tbl";
open(LINEITEM, "<lineitem.tbl") or die "Can't open region.tbl";
open(LINEITEM1, ">lineitem1.tbl") or die "Can't open region1.tbl";
while ( <LINEITEM> )
{	
	chomp;
	(@token) = split /\|/ ;
	print LINEITEM1 $token[0], ", ", $token[1], ", ", $token[2], ", ", $token[3], ", ", $token[4], ", ", $token[5], ", ", $token[6], ", ", $token[7], ", '", $token[8], "', '", $token[9], "', '", $token[10], "', '", $token[11], "', '", $token[12], "', '", $token[13], "', '", $token[14], "', '", $token[15],"'\n";
}
close(LINEITEM);
close(LINEITEM1);
unlink "lineitem.tbl";
rename "lineitem1.tbl", "lineitem.tbl";