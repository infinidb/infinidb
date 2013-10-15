<html>
<head><title><?php echo $_GET["title"]; ?></title></head>
<body>
<?php
echo "<pre>\n";
$file = fopen($_GET["logFile"], "r") or exit("Unable to open file!");
//Output a line of the file until the end is reached
while(!feof($file))
{
  $line = fgets($file);
  if (!preg_match('/ Failed  *= 0/', $line))
          $line = preg_replace('/ Failed/', ' <font color="red"><b>Failed</b></font>', $line);
  $line = preg_replace('/ Passed/', ' <font color="green"><b>Passed</b></font>', $line);
  $line = preg_replace('/ Yellow/', ' <font color="yellow"><b>Yellow</b></font>', $line);
  echo $line;
}
fclose($file);
echo "</pre>\n";
?>
</body>
</html>
