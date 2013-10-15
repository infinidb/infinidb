#!/bin/bash
# $Id$
#
# displays the changes in git develop for yesterday

usage()
{
	echo "usage: nightlyDiff.sh [-sfh] [-d days] [-b branch]"
	echo "   -h        display this help"
	echo "   -s        display only a summary of the files changed"
	echo "   -f        display fancy diffstat"
	echo "   -d days   go back days days"
#	echo "   -b branch display changes for branch branch instead of develop"
}

urlencode()
{
	local arg
	arg="$1"
	while [[ "$arg" =~ ^([0-9a-zA-Z/:_\.\-]*)([^0-9a-zA-Z/:_\.\-])(.*) ]] ; do
		echo -n "${BASH_REMATCH[1]}"
		printf "%%%X" "'${BASH_REMATCH[2]}'"
		arg="${BASH_REMATCH[3]}"
	done
	echo -n "$arg"
}

DAYSAGO=1
BRANCH=develop
while getopts ":sfhd:b:" options; do
	case $options in
	s ) SUMMARY_ONLY="--name-only" ;;
	f ) FANCY="--stat" ;;
	d ) DAYSAGO=$OPTARG ;;
	b ) BRANCH=$OPTARG ; echo "-b doesn't work yet" 1>&2; exit 1 ;;
	h ) usage; exit 0 ;;
	* ) usage; exit 1 ;;
	esac
done

secs=$(($(date +%s)-60*60*24*DAYSAGO))

dt1=$(date --date=@$secs +%Y-%m-%d)

BRANCH=$(urlencode "$BRANCH")

if [ $BRANCH != develop ]; then
	BRANCH=branches/$BRANCH
fi

git diff $SUMMARY_ONLY $FANCY "$BRANCH@{${dt1} 00:00:00}..$BRANCH@{${dt1} 23:59:59}"

