from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
import math


#check if there is any NAs in the selected feature
#also delete cancelled flight which is in line[21]
def IsNotNAorCancelled(line, selcol):
	for col in selcol:
		if line[col] == 'NA':
			return False
	if line[21] !="0":
		return False
	return True


#load data and remove header
def prep_Data(path, selcol):
    data_wh = sc.textFile(path)#with header
    header = data_wh.first()
    data = data_wh.filter(lambda line: line!= header)\
                .map(lambda line: line.split(","))\
                .filter(lambda line: IsNotNAorCancelled(line, selcol))
    print data.first()
    print "With " + str(data.count()) + " lines of data."
    return data


#extract feature and labels
def extract_features(line):
    feature = [int(line[1]), int(line[3]), int(line[4]), int(line[6]), int(line[18]), str(line[23])]
    return (feature)
    
def extract_label(line):
    label = (line[25])
    return float(label)


#calculate mae and rmse
def evaluateModel(model, validationData):
    score = model.predict(validationData.map(lambda p: p.features))
    scoreAndLabels=score.zip(validationData.map(lambda p: p.label))
    mae = scoreAndLabels.map(lambda (a, b): math.fabs(a-b)).mean()
    mse = scoreAndLabels.map(lambda (a, b): (a-b)**2).mean()
    rmse = math.sqrt(mse)
    #metrics = RegressionMetrics(scoreAndLabels)
    #RMSE=metrics.rootMeanSquaredError
    return mae, rmse

sc = SparkContext()
	
selcol = [1,3,4,6,18,23,25]
train = prep_Data("HW4/200[3-7].csv", selcol)
test = prep_Data("HW4/2008.csv", selcol)


#transform data into the format that can be feed into model
trainLabeled = train.map(lambda line: LabeledPoint(
                                                   extract_label(line),
                                                   extract_features(line)
                                                   ))
testLabeled = test.map(lambda line: LabeledPoint(
                                                   extract_label(line),
                                                   extract_features(line)
                                                   ))


#preserver some part of the data as validation data
train_dataset, val_dataset = trainLabeled.randomSplit([0.7, 0.3])


#train
linear_model_val = LinearRegressionWithSGD.train(train_dataset,100000,0.00000000001)
linear_model = LinearRegressionWithSGD.train(trainLabeled,100000,0.00000000001)


#evaluateModel(linear_model_val, val_dataset)
#evaluateModel(linear_model, testLabeled)

#evaluate data
mae_val, rmse_val = evaluateModel(linear_model_val, val_dataset)
mae, rmse = evaluateModel(linear_model, testLabeled)


print "Validation: \n"+"MAE: "+str(mae_val)+"\nRMSE: "+str(rmse_val)
print "\nTest: \n"+"MAE: "+str(mae)+"\nRMSE: "+str(rmse)



