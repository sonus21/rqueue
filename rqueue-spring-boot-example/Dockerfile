FROM tomcat:8.0.51-jre8-alpine
RUN rm -rf /usr/local/tomcat/webapps/*
ARG WAR_FILE=build/libs/*.war
COPY ${WAR_FILE} /usr/local/tomcat/webapps/ROOT.war
CMD ["catalina.sh","run"]