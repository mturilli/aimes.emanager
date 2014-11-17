# Batch script to run the AIMES SC2014 demo. Before running it, run the setup
# script 'demo_SC2014_setup.sh. Run the demo from a shell with:
#
# . demo_SC2014.sh

. `which demo_SC2014_env_setup.sh`

cat > description.log <<EOL
AIMES SC2014 Demo

Libraries
---------
Radical pilot: `radicalpilot-version`
Saga python: `sagapython-version`
Radical utils: `python -c "import radical.utils as ru; print ru.version"`

Infrastructure
--------------
Resources: ${RADICAL_PILOT_RESOURCES}
DBURL: ${RADICAL_PILOT_DBURL}

EOL

which unbuffer > /dev/null
if test $? = 0; then
  unbuffer demo_SC2014_script.py 2>&1 | tee out.log
else
  demo_SC2014_script.py 2>&1 | tee out.log
fi

SESSION_UID=`grep 'created        : UID' out.log | cut -d ' ' -f 13`
radicalpilot-stats -m plot,stat -s $SESSION_UID > stats.out 2>/dev/null

tar cfj out.log.bz2 out.log
cat description.log | mutt -a "${SESSION_UID}.png" -a "stats.out" -s "[Experiment] $WORKLOAD_BAG_SIZE tasks - Session UID $SESSION_UID" -- matteo.turilli@gmail.com,andre@merzky.net,shantenu.jha@rutgers.edu
