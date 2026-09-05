# The add-on image (docs/07 §5.3; docs/12 WP18).
#
# Built FROM the published multi-arch image rather than rebuilding the bridge, so the add-on can
# never ship a different build from the container users run directly -- one artefact, one set of
# verified behaviour.
ARG BUILD_FROM=ghcr.io/steelnight/freeathome2mqtt:latest
FROM ${BUILD_FROM}

USER root
COPY run.sh /run.sh
RUN chmod +x /run.sh

# The Supervisor's own healthcheck supersedes the base image's, which assumes it can reach the
# broker on its own terms.
HEALTHCHECK NONE

ENTRYPOINT []
CMD ["/run.sh"]
