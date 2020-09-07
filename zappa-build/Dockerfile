# This is a re-usable image used to deploy zappa applications
# with native dependencies.
# Zappa makes an attempt to support 'cross-compilation' on a Mac,
# but this doesn't always work because it relies on the Zappa maintainers
# updating lambda-packages, which they are not doing anymore.
# See ../zappa-deploy.sh
#
# Based on:
# https://romandc.com/zappa-django-guide/setup/#approach-2-docker-with-zappa-recommended

FROM lambci/lambda:build-python3.8

# Zappa relies on the virtual env existing in the project directoy
ENV PIPENV_VENV_IN_PROJECT=true

# Fancy prompt to remind you are in zappashell
RUN echo 'export PS1="\[\e[36m\]zappashell>\[\e[m\] "' >> /root/.bashrc

RUN yum install -y python-devel postgresql-devel rsync

CMD ["bash"]
