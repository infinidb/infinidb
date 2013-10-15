#!/bin/bash
#
# Determine the Linux distribution and version that is being run.
#
# Check for GNU/Linux distributions
   if [ -f /etc/SuSE-release ]; then
     DISTRIBUTION="suse"
   elif [ -f /etc/UnitedLinux-release ]; then
     DISTRIBUTION="united"
  elif [ -f /etc/debian_version ]; then
    DISTRIBUTION="debian"
  elif [ -f /etc/lsb_version ]; then
    DISTRIBUTION="ubuntu"
  elif [ -f /etc/redhat-release ]; then
    a=`grep -i 'red.*hat.*enterprise.*linux' /etc/redhat-release`
    if test $? = 0; then
      DISTRIBUTION=rhel
    else
      a=`grep -i 'red.*hat.*linux' /etc/redhat-release`
      if test $? = 0; then
        DISTRIBUTION=rh
      else
        a=`grep -i 'Fedora' /etc/redhat-release`
        if test $? = 0; then
          DISTRIBUTION=fedora
        else
          a=`grep -i 'cern.*e.*linux' /etc/redhat-release`
          if test $? = 0; then
            DISTRIBUTION=cel
          else
            a=`grep -i 'scientific linux cern' /etc/redhat-release`
            if test $? = 0; then
              DISTRIBUTION=slc
            else
              DISTRIBUTION="unknown"
            fi
          fi
        fi
      fi
    fi
  else
    DISTRIBUTION="unknown"
  fi
echo ${DISTRIBUTION}
