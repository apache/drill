---
title: "Launch Drill Under YARN"
date:  
parent: "Drill-on-YARN"
---  

Use the client tool to launch your new Drill cluster, as shown:  

              $DRILL_HOME/bin/drill-on-yarn.sh site
              $DRILL_SITE start


A number of lines describing the start-up process appear. The tool automatically archives
and uploads your site directory, which YARN copies (along with Drill) onto each node. If all goes well, the tool prints a URL for the Drill Application Master process that you can use to monitor the cluster. Your Drillbits should now be up and running. (If not, see the Troubleshooting section.)  

Check the status of your Drill cluster, as shown:  

              $DRILL_HOME/bin/drill-on-yarn.sh site
              $DRILL_SITE status  

Stop your cluster, as shown:  

              $DRILL_HOME/bin/drill-on-yarn.sh site
              $DRILL_SITE stop


Note, to avoid typing the site argument each time, you can set an environment variable, as shown:  

              export DRILL_CONF_DIR=$DRILL_SITE
              $DRILL_HOME/bin/drill-on-yarn.sh start