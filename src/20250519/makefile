TAG=v1
ACCOUNT=u03013112
IMAGE=${ACCOUNT}/pymc:${TAG}

all:
	docker build -t ${IMAGE} .
push:
	docker push ${IMAGE} 