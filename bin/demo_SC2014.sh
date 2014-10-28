# Batch script to run the AIMES SC2014 demo. Before running it, run the setup
# script 'demo_SC2014_setup.sh. Run the demo from a shell with:
#
# . demo_SC2014.sh

export SKELETON_CONF="etc/skeleton_demo_SC2014.conf"
export BUNDLE_CONF="etc/bundle_demo_SC2014.conf"

cat > description.log <<EOL
AIMES SC2014 Demo

Libraries
---------
Radical pilot: `radicalpilot-version`
Saga python: `python -c "import saga; print saga.version"`

Infrastructure
--------------
Resources: ${RADICAL_PILOT_RESOURCES}
DBURL: ${RADICAL_PILOT_DBURL}
Allocation: ${XSEDE_PROJECT_ID}

EOL

unbuffer demo_SC2014_script.py 2>&1 | tee out.log

SESSION_UID=`grep 'Session UID:' out.log | cut -d ' ' -f 3`
radicalpilot-stats -m plot,stat -s $SESSION_UID > stats.out

tar cfj out.log.bz2 out.log
cat description.log | mutt -a "${SESSION_UID}.png" -a "stats.out" -s "[Experiment] $WORKLOAD_BAG_SIZE tasks - Session UID $SESSION_UID" -- matteo.turilli@gmail.com,andre@merzky.net
