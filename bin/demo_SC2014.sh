# Batch script to run the AIMES SC2014 demo. Before running it, run the setup
# script 'demo_SC2014_setup.sh. Run the demo from a shell with:
#
# . demo_SC2014.sh
#
# Author: Matteo Turilli, Andre Merzky
# copyright: Copyright 2014, RADICAL
# license: MIT

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

# Write the body of the report e-mail
cat > description.log <<EOL
${RUN_TAG}

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

# Archive the run.
ARCHIVE_DIR=$N_TASKS-$SESSION_UID

mkdir $ARCHIVE_DIR

mv out.log             $ARCHIVE_DIR
mv stats.out           $ARCHIVE_DIR
mv description.log     $ARCHIVE_DIR
mv $RADICAL_DEBUG_FILE $ARCHIVE_DIR
mv $SESSION_UID.png    $ARCHIVE_DIR
mv $SESSION_UID.pdf    $ARCHIVE_DIR

# Send the e-mail with the information, stats, diagram of the run.
cat $ARCHIVE_DIR/description.log |                            \
mutt -a "$ARCHIVE_DIR/$SESSION_UID.png"                       \
     -a "$ARCHIVE_DIR/stats.out"                              \
     -a "$ARCHIVE_DIR/log.out"                                \
     -s "[$RUNTAG] $N_TASKS tasks - Session UID $SESSION_UID" \
     -- $RECIPIENTS
