package main

import (
	"github.com/lann/builder"
)

type SparkSubmit struct {
	SparkMaster string
	CassandraHost string
	Script string
	MappingConfiguration string
	Packages string
	fileLocation string
	}

type sparkCommandBuilder builder.Builder

func (b sparkCommandBuilder) SparkMaster(sparkMaster string) sparkCommandBuilder {
    return builder.Set(b, "SparkMaster", sparkMaster).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) CassandraHost(cassandraHost string) sparkCommandBuilder {
    return builder.Set(b, "CassandraHost", cassandraHost).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) Script(script string) sparkCommandBuilder {
    return builder.Set(b, "Script", script).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) MappingConfiguration(mappingConfiguration string) sparkCommandBuilder {
    return builder.Set(b, "MappingConfiguration", mappingConfiguration).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) Packages(packages string) sparkCommandBuilder {
    return builder.Set(b, "Packages", packages).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) FileLocation(fileLocation string) sparkCommandBuilder {
    return builder.Set(b, "FileLocation", fileLocation).(sparkCommandBuilder)
}

func (b sparkCommandBuilder) Build() SparkSubmit {
    return builder.GetStruct(b).(SparkSubmit)
}

var SparkCommandBuilder = builder.Register(sparkCommandBuilder{}, SparkSubmit{}).(sparkCommandBuilder)