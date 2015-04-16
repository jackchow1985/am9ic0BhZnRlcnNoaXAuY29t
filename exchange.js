var bs = require('nodestalker'),
    //client = bs.Client('challenge.aftership.net:11300'),
    cheerio = require('cheerio'),
    mongo = require('mongoskin'),
    request = require('request');

var beanstalkd_url = "challenge.aftership.net:11300";
var error_retry_times = 0;
var success_times = 0; 
var mongodb = mongo.db("mongodb://jack:asdfasdf@ds061681.mongolab.com:61681/exchange?auto_reconnect=true"); //mongodb url
var collection = mongodb.collection("rate"); //mongodb collection name
var tube_name = "jackchow1985"

//Generate task in the working q, do not invoke in the worker process.
// function createTask() {
// 	var client = bs.Client('challenge.aftership.net:11300');
// 	client.use('jackchow1985').onSuccess(function(data) {
// 	    console.log(data);

// 	  	client.put(JSON.stringify({
// 		  "from": "HKD",
// 		  "to": "USD"
// 		})).onSuccess(function(data) {
// 		    console.log(data);
// 		    client.disconnect();
// 		});
// 	});
// }

function notifyWorker() {
	var client = bs.Client(beanstalkd_url); // nodestalker implementation, blocking process.
	client.watch(tube_name).onSuccess(function(data) {
	    client.reserve().onSuccess(function(job) {
	        console.info("Get task from q: " + job.data);
	        getExchangeRate(JSON.parse(job.data), function(errCode, msg) {
	        	client.disconnect();
	        	if(!errCode && success_times < 10) { // task normal, wait for 1 min
	        		success_times ++;	        		
		        	setTimeout(function() {
		        		//reput the job to q
		        		client.put(job.data).onSuccess(function(data) {
				 		    client.disconnect();
				 		    notifyWorker();
				 		});		        		
		        	}, 1000*60) // wait 1 min
		        } else if(errCode && error_retry_times < 3) { //retry, task error
					retry_times ++;
					setTimeout(function() {
						//reput the job to q
		        		client.put(job.data).onSuccess(function(data) {
				 		    client.disconnect();
				 		    notifyWorker();
				 		});
		        	}, 1000*3) // wait 3s
		        } else {
		        	// Do nothing, stop.
		        	process.exit(0); //exit with success
		        }
	        });
	        //delete the processed job
	        client.deleteJob(job.id).onSuccess(function(del_msg) {
	            console.log('deleted', job);
	        });
	    });
	});
}

function _convertRate(rate) {
	if(rate) {
		rate = rate.split(" ")[0];
		if(_isNumber(rate)) {
			return parseFloat(rate).toFixed(2);
		}
	} else {	
		return rate
	}
}

function _isNumber(n) {
  return !isNaN(parseFloat(n)) && isFinite(n);
}

function getExchangeRate(taskObj, callback) {

	request.get("http://www.xe.com/currencyconverter/convert/?Amount=1&From=" + taskObj.from + "&To=" + taskObj.to + "&r=#converter", function(err, res, html) {
		//console.info(html)
		if(!err && html) {
			var $ = cheerio.load(html);
			if($(".rightCol") && $(".rightCol").length > 0 && $(".rightCol").eq(0).text()) { // successful found the rate
				var exchange_rate = _convertRate($(".rightCol").eq(0).text());
				console.info(exchange_rate)
				var saveObj = {
				    "from": taskObj.from,
				    "to": taskObj.to,
				    "created_at": Date.parse(new Date()),
				    "rate": exchange_rate
				}
				//save to mongodb
				collection.save(saveObj, function(err, savedObj) {
		            if(err) {
		                console.error(err);		                
		            } else {
		            	hasNew = true;
		            	//log the saved object to verify
		                console.info(" ****** Saved to DB  ********")
		                for(var m in savedObj) {
		                    console.info(m  + ": " + savedObj[m]);
		                }
		                console.info(" ****************************")
		            }
		            callback()
		        })
			} else { // can not get the targeted rate
				callback(400, "Can not get the targeted rate")
			}
		} else { // can not load the page, typically, network issue or server refused			
			callback(500, "Network problem");			
		}	

	});
}

notifyWorker();

//createTask();
