#create a granafa image with custom configuration
FROM grafana/grafana:latest

ENV GF_AUTH_ANONYMOUS_ENABLED=true
ENV GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
ENV GF_AUTH_BASIC_ENABLED=false
#ENV GF_AUTH_DISABLE_LOGIN_FORM=true
#ENV GF_AUTH_DISABLE_SIGNOUT_MENU=true
ENV GF_SECURITY_ALLOW_EMBEDDING=true
#ENV GF_SERVER_SERVE_FROM_SUB_PATH=true
ENV GF_SERVE_FROM_SUB_PATH=true
ENV GF_SECURITY_ADMIN_USER=admin
ENV GF_SECURITY_ADMIN_PASSWORD=admin
ENV GF_USERS_ALLOW_SIGN_UP=false

EXPOSE 3000

ADD ./provisioning /etc/grafana/provisioning
ADD ./dashboards /var/lib/grafana/dashboards

USER root
RUN chown -R 472 /etc/grafana/provisioning
USER 472

CMD [ "grafana-reporter" ]
