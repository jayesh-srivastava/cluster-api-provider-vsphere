#!/bin/bash


#rm generated/*

kustomize build --load_restrictor none core/global > generated/core-global.yaml
kustomize build --load_restrictor none core/base > generated/core-base.yaml
