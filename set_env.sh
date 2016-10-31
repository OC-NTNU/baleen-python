# To setup a local environment for Baleen, execute command
#
#    source set_env.sh
#
source activate baleen
BALEEN_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$PYTHONPATH:$BALEEN_HOME/lib
export BALEEN_INI=$BALEEN_HOME/etc/local.ini
