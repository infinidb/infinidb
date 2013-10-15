<html>
<head>
<meta http-equiv="refresh" content="60" >
</head>
<body>
<?php
header('Cache-Control: no-cache, no-store, must-revalidate'); // HTTP 1.1. 
header('Pragma: no-cache'); // HTTP 1.0. 
header('Expires: 0'); // Proxies.
$con = mysql_connect('localhost','root','',false,65536);

mysql_select_db ("stacks");

$query="call list_stacks()"; 
/* $result=mysql_query("call list_stacks()"); */
$result=mysql_query($query)
or die(mysql_error());

$num=mysql_numrows($result);

mysql_close();
?>
User:  <input id="userInput" value="<?php echo $_GET["user"]; ?>" />&nbsp;<button onClick="javascript:refresh();">Refresh</button>
<form action="stackReservationResult.php" method="post" id="form1">
<input type="hidden" name="stack" id="stack" value=""/>
<input type="hidden" name="action" id="action" value=""/>
<input type="hidden" name="user" id="user" value="<?php echo $_GET["user"]; ?>"/>
<br>
<table border="1" cellspacing="2" cellpadding="2">
<tr>
<th align=left><font face="Arial, Helvetica, sans-serif">&nbsp;</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">&nbsp;</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">Stack</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">User Module</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">Status</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">User</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">Checked Out</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">Time</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">Stack Notes</font></th>
<th align=left><font face="Arial, Helvetica, sans-serif">Version</font></th>
</tr>

<?php
$i=0;
while ($i < $num) {

$f3=mysql_result($result,$i,"stack");
$f4=mysql_result($result,$i,"user module");
$f5=mysql_result($result,$i,"status");
$f6=mysql_result($result,$i,"user");
$f7=mysql_result($result,$i,"checked out");
$f8=mysql_result($result,$i,"time");
$f9=mysql_result($result,$i,"notes");
$f10=mysql_result($result,$i,"version");

if($f5 == "Available") {
    $f1 = "Reserve";
    $f2 = "";
}
else if($f5 == "Checked Out" && $f6 == $_GET["user"]) {
    $f1 = "";
    $f2 = "Release";
}
else {
    $f1="";
    $f2="";
}
?>

<script type="text/javascript">
function submitForm(action, stack)
{
    document.getElementById('stack').value=stack;
    document.getElementById('action').value=action;
    document.getElementById('user').value=document.getElementById('userInput').value;
    document.getElementById('form1').submit();
}

function refresh()
{
    window.location='stack.php?user=' + document.getElementById('userInput').value;
}
</script>


<tr>
<td><font face="Arial, Helvetica, sans-serif"><a href="javascript:submitForm('reserve', '<?php echo $f3; ?>');"><?php echo $f1; ?></a>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><a href="javascript:submitForm('release', '<?php echo $f3; ?>');"><?php echo $f2; ?></a>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f3; ?>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f4; ?>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f5; ?>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f6; ?>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f7; ?>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f8; ?>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f9; ?>&nbsp;&nbsp;&nbsp;</font></td>
<td><font face="Arial, Helvetica, sans-serif"><?php echo $f10; ?>&nbsp;&nbsp;&nbsp;</font></td>
</tr>

<?php
$i++;
}
?>
</table>
</form>
</body>
</html>
