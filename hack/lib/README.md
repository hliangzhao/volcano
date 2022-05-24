Explanations:

* `golang.sh`: This script is used to verify that the right version of golang is installed.
* `util.sh`: This script is used to capture signals and execute commands when running other shell scripts.
* `init.sh`: Run `golang.sh` and `util.sh`.
* `install.sh`: This script is used to check kind, kubectl, helm and the volcano docker images exist or not. If not, install then.
  The script prepares testing environment.