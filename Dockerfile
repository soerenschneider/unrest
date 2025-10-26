FROM golang:1.25.1 AS restic

# renovate: datasource=github-releases depName=restic/restic
ARG RESTIC_VERSION=v0.18.1

RUN apt update && \
    apt -y install git && \
    git clone https://github.com/restic/restic /restic && \
    git -C /restic checkout ${RESTIC_VERSION}

WORKDIR /restic
RUN CGO_ENABLED=0 go run helpers/build-release-binaries/main.go -p linux/amd64 --skip-compress

FROM golang:1.25.1 AS resticprofile
# renovate: datasource=github-releases depName=creativeprojects/resticprofile
ARG RESTICPROFILE_VERSION=v0.32.0
RUN CGO_ENABLED=0 go install github.com/creativeprojects/resticprofile@${RESTICPROFILE_VERSION}

FROM golang:1.25.1 AS unrest

COPY go.mod go.sum /unrest/
WORKDIR /unrest
RUN go mod download

COPY . /unrest

RUN CGO_ENABLED=0 go build restic_restore.go

FROM alpine:3.22.1

ARG USERNAME=restic
ARG USER_UID=16523
ARG USER_GID=$USER_UID
ARG POSTGRES_MAJOR=17

RUN addgroup --gid "$USER_GID" "$USERNAME" && \
    adduser --disabled-password --ingroup "$USERNAME" --uid "$USER_UID" $USERNAME

RUN apk add --no-cache gzip mariadb-client postgresql${POSTGRES_MAJOR}-client sqlite

COPY --from=restic /output/restic_linux_amd64 /usr/local/bin/restic
COPY --from=resticprofile /go/bin/resticprofile /usr/local/bin/resticprofile
COPY --from=unrest /unrest/restic_restore /usr/bin/restic_restore
