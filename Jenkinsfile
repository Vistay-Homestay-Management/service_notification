pipeline {
    agent any

    triggers {
        pollSCM('* * * * *')
    }

    options {
        skipDefaultCheckout(true)
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        IMAGE_NAME = 'service_notification'
        DEV_IMAGE_TAG = 'dev'
        DOCKER_CONTEXT = '.'
        DOCKERFILE_PATH = 'Dockerfile'
        DOCKER_BUILD_ARGS = ''
        ADDITIONAL_IMAGE_TAG = ''
    }

    stages {
        stage('Checkout') {
            steps {
                checkout([
                    $class: 'GitSCM',
                    branches: scm.branches,
                    doGenerateSubmoduleConfigurations: false,
                    extensions: [[
                        $class: 'SubmoduleOption',
                        disableSubmodules: false,
                        parentCredentials: true,
                        recursiveSubmodules: true,
                        reference: '',
                        trackingSubmodules: false
                    ]],
                    userRemoteConfigs: scm.userRemoteConfigs
                ])
            }
        }

        stage('Prepare local image tags') {
            steps {
                script {
                    def branch = env.BRANCH_NAME ?: env.GIT_BRANCH ?: ''
                    branch = branch.replaceFirst('^origin/', '')
                    branch = branch.replaceFirst('^refs/heads/', '')

                    if (branch == 'main') {
                        env.TARGET_TAG = 'latest'
                    } else if (branch == 'dev') {
                        env.TARGET_TAG = env.DEV_IMAGE_TAG
                    } else if (branch == 'test') {
                        env.TARGET_TAG = 'test'
                    } else {
                        env.TARGET_TAG = ''
                    }

                    env.COMMIT_TAG = 'sha-' + sh(
                        script: 'git rev-parse HEAD',
                        returnStdout: true
                    ).trim()
                    env.LOCAL_IMAGE_TAG = 'ci-' + env.BUILD_NUMBER

                    echo('Branch: ' + branch)
                    echo('Local branch tag: ' + (env.TARGET_TAG ?: '(none)'))
                    echo('Local commit tag: ' + env.COMMIT_TAG)
                }
            }
        }

        stage('SonarQube analysis') {
            steps {
                withSonarQubeEnv('SonarQube') {
                    sh '''
                        set +x
                        export SONAR_TOKEN="$SONAR_AUTH_TOKEN"
                        docker run --rm \
                            --network container:cmc-jenkins \
                            --volumes-from cmc-jenkins \
                            --volume cmc-jenkins-gradle:/root/.gradle \
                            --workdir "$WORKSPACE" \
                            --env SONAR_HOST_URL \
                            --env SONAR_TOKEN \
                            eclipse-temurin:21-jdk-jammy \
                            sh -ec 'sh ./gradlew classes testClasses sonar -x test --no-daemon'
                    '''
                }
            }
        }

        stage('SonarQube Quality Gate') {
            steps {
                timeout(time: 15, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }

        stage('Build Docker image locally') {
            steps {
                sh 'docker build --pull $DOCKER_BUILD_ARGS --file "$DOCKERFILE_PATH" --tag "$IMAGE_NAME:$LOCAL_IMAGE_TAG" "$DOCKER_CONTEXT"'
                script {
                    if ((env.TARGET_TAG ?: '').trim()) {
                        sh 'docker tag "$IMAGE_NAME:$LOCAL_IMAGE_TAG" "$IMAGE_NAME:$TARGET_TAG"'
                        sh 'docker tag "$IMAGE_NAME:$LOCAL_IMAGE_TAG" "$IMAGE_NAME:$COMMIT_TAG"'
                    }
                    if ((env.ADDITIONAL_IMAGE_TAG ?: '').trim()) {
                        sh 'docker tag "$IMAGE_NAME:$LOCAL_IMAGE_TAG" "$IMAGE_NAME:$ADDITIONAL_IMAGE_TAG"'
                    }
                    if ((env.TARGET_TAG ?: '').trim() || (env.ADDITIONAL_IMAGE_TAG ?: '').trim()) {
                        sh 'docker image rm "$IMAGE_NAME:$LOCAL_IMAGE_TAG"'
                    }
                }
            }
        }
    }
}

