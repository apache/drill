---
title: "Enabling Web UI Security"
date: 2018-11-02
parent: "Drill-on-YARN"
---  

Drill-on-YARN provides a web UI as described earlier. By default, the UI is open to everyone.
You can secure the UI using either a simple predefined user name and password, or using
Drill’s user authentication.  

##Simple Security
Simple security is enabled using three configuration settings:  

       drill.yarn.http: {
              auth-type: "simple"
              user-name: "bob"
              password: "secret"
       }  

Restart the Drill-on-YARN Application Master. When you visit the web UI, a login page should
appear, prompting you to log in. Only the above user and password are valid. Simple security is not highly secure; but it is useful for testing, prototypes and the like.  

##Using Drill’s User Authentication
Drill-on-YARN can use Drill’s authentication system. In this mode, the user name and password
must match that of the user that started the Drill-on-YARN application. To enable Drill security:  

       drill.yarn.http: {
              auth-type: "drill"
        }  

You must have previously enabled Drill user authentication, as described in the [Drill
Documentation]({{site.baseurl}}/docs/configuring-user-authentication/) .