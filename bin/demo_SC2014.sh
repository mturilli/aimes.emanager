# Batch script to run the AIMES SC2014 demo. Before running it, run the setup
# script 'demo_SC2014_setup.sh. Run the demo from a shell with:
#
# . demo_SC2014.sh

# Load the environment variables required when running the demo.
. `which demo_SC2014_env_setup.sh`

# Direct to STDOUT and to the file out.log.
which unbuffer > /dev/null
if test $? = 0; then
  unbuffer demo_SC2014_script.py 2>&1 | tee out.log
else
  demo_SC2014_script.py 2>&1 | tee out.log
fi

# Get some information from the logs.
SESSION_UID=`grep 'created        : UID' out.log | cut -d ' ' -f 13`
RADICAL_PILOT_RESOURCES=`grep 'IDs: ' out.log | cut -d ':' -f 2 | sed 's/^ \(.*\)/\1/'`
N_TASKS=`grep 'Total number of tasks' out.log | cut -d ':' -f 2 | sed 's/^ *\(.*\)/\1/'`
N_STAGES=`grep 'Total number of stages' out.log | cut -d ':' -f 2 | sed 's/^ *\(.*\)/\1/'`
I_DATA=`grep 'Total input data' out.log | cut -d ':' -f 2 | sed 's/^ *\(.*\)/\1/'`
O_DATA=`grep 'Total output data' out.log | cut -d ':' -f 2 | sed 's/^ *\(.*\)/\1/'`

# Produce diagrams and statistics for the run.
radicalpilot-stats -m plot,stat -s $SESSION_UID > stats.out 2>/dev/null

# Archive the logs.
tar cfj out.log.bz2 out.log
cp -p $RADICAL_DEBUG_FILE "$RADICAL_DEBUG_FILE-$SESSION_UID"

# Write the body of the e-mail.
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

Worflow
-------
Total number of stages: ${N_STAGES}
Total number of tasks: ${N_TASKS}
Total input data: ${I_DATA}
Total output data: ${O_DATA}

EOL

cat stats.out | sed -e '1,/plotting.../d' >> description.log

# Send the e-mail with the information, stats, diagram of the run.
cat description.log | mutt -a "${SESSION_UID}.png" -a "stats.out" -s "[AIMES demo SC2014] $N_TASKS tasks - Session UID $SESSION_UID" -- matteo.turilli@gmail.com,andre@merzky.net,shantenu.jha@rutgers.edu
