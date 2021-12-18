package main

type FeatureName string

type FeatureValue float64

type Feature struct {
	name  FeatureName  // the name of the feature e.g. avgValue
	value FeatureValue // the value of the feature e.g. 34.6
	id    string       // what entity is this a feature of e.g. abc123
}

type FeatureVector struct {
	id       string                  // what entity does this feature vector correspond to e.g. abc123
	features map[FeatureName]float64 // the feature values
	names    []FeatureName           // what order do the features come in?
}
