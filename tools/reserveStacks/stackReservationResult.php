<html>
<head>
</head>
<body>
<?php
header('Cache-Control: no-cache, no-store, must-revalidate'); // HTTP 1.1. 
header('Pragma: no-cache'); // HTTP 1.0. 
header('Expires: 0'); // Proxies.
$con = mysql_connect('localhost','root','',false,65536);

mysql_select_db ("stacks");

/*
Reserve or release the stack if the user clicked a link to do said reservation wherefore art thou.
*/
$stack=$_POST["stack"];
$action=$_POST["action"];
$who=$_POST["user"];
if($stack != "" && $who != "") {
    if($action=="reserve") {
        $query="call reserve_stack2('" . $stack . "', '". $who . "', 'php');"; 
    }
    else {
        $query="call release_stack('" . $stack . "', '". $who . "');";
    }
    $result = mysql_query($query) or die(mysql_error()); 
    if($result) {
        $line = mysql_result($result,0,"status");
        if (stripos($line, 'not')) {
            echo '<b><font color="red">';
        }
        else {
            echo '<b><font color="green">';
        }
        echo $line;
        echo '</font></b><br><br>';
    }
}
mysql_close();
echo '<a href="stack.php?user=' . $who . '">Return to Stacks</a>';
?>


</body>
</html>
