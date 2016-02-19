package main

import (
	"github.com/lann/builder"
)

type SparkSubmit struct {
	SparkMaster string
	CassandraHost string
	MinioHost string
	FileLocation string
	TrialID string
	ContainerID string
	SUTName string
	Script string
	Files string
	PyFiles string
	Packages string
	}

type sparkCommandBuilder builder.Builder

func (b sparkCommandBuilder) SparkMaster(sparkMaster string) sparkCommandBuilder {
    return builder.Set(b, "SparkMaster", sparkMaster).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) CassandraHost(cassandraHost string) sparkCommandBuilder {
    return builder.Set(b, "CassandraHost", cassandraHost).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) MinioHost(minioHost string) sparkCommandBuilder {
    return builder.Set(b, "MinioHost", minioHost).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) TrialID(trialID string) sparkCommandBuilder {
    return builder.Set(b, "TrialID", trialID).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) ContainerID(containerID string) sparkCommandBuilder {
    return builder.Set(b, "ContainerID", containerID).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) Script(script string) sparkCommandBuilder {
    return builder.Set(b, "Script", script).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) Files(files string) sparkCommandBuilder {
    return builder.Set(b, "Files", files).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) PyFiles(pyFiles string) sparkCommandBuilder {
    return builder.Set(b, "PyFiles", pyFiles).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) Packages(packages string) sparkCommandBuilder {
    return builder.Set(b, "Packages", packages).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) FileLocation(fileLocation string) sparkCommandBuilder {
    return builder.Set(b, "FileLocation", fileLocation).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) SUTName(SutName string) sparkCommandBuilder {
    return builder.Set(b, "SUTName", SutName).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) Build() SparkSubmit {
    return builder.GetStruct(b).(SparkSubmit)
}

var SparkCommandBuilder = builder.Register(sparkCommandBuilder{}, SparkSubmit{}).(sparkCommandBuilder)