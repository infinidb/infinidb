/BufferStatus/ {
    pix = $2/96;
    # print pix;
    str = "";
    if($3 < $4) {
        for(i=0; i < $2; i= i + pix) {
        #   print i, $2, $3, $4;
        #       printf("/");
           if(i < $3) {
               str = str "*";
           } else if (i >= $3 && i < $4) {
               str = str "-";
           } else if (i >= $4) {
               str = str "*";
           }
        }
        
    } else {
        for(i=0; i < $2; i = i + pix) {
        #   print i, $2, $3, $4;
        #       printf("\\");
           if(i < $4) {
               str = str "-";
           } else if (i >= $4 && i < $3) {
               str = str "*";
           } else if (i >= $3) {
               str = str "-";
           }
        }
    }
    printf("|%s| - %d\n", str, $2);
}
