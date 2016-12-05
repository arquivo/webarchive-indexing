# This script creates for each Job (collection)
# File named AllErrors.txt with all generated errors from systderr
# File named FailedArcsUnique.txt with the list of Arcs that failed indexing to CDX
for dir in /opt/hadoop-1.2.1/logs/joblogs/userlogs/*/
do
    dir=${dir%*/}
    cd ${dir##*/}
    rm -f AllErrors.txt
    rm  -f FailedArcs.txt
    rm  -f FailedArcsUnique.txt 
    echo ${dir##*/}
	find . -name "stderr" -exec cat {} \; > AllErrors.txt
	grep 'Arcname:' AllErrors.txt > FailedArcs.txt
	sort FailedArcs.txt | uniq > FailedArcsUnique.txt    
    cd ..    
done