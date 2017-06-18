exports.handler = (event, context, callback) => {
  var AWS = require('aws-sdk');
  var in_array = require('locutus/php/array/in_array');
  var array_keys = require('locutus/php/array/array_keys');

  var rds_query_function = process.env.RDS_QUERY_FUNCTION;

  var currencies = ['BTC-USD','ETH-USD','LTC-USD'];
  var types = ['candles','prices'];
  var output_formats = ['default','highcharts'];

  var input = {}; 
  var isApiProxy = false;

  var request_response = {
    statusCode: 200,
    headers: {},
    body: null
  };

  var lambda = new AWS.Lambda();

  if ((event.queryStringParameters !== null) && (event.queryStringParameters !== undefined)) {
    input = event.queryStringParameters;
    isApiProxy = true;
  } else {
    input = event;
    isApiProxy = false;
  }
  
  var output_format = 'default';
  if (typeof input.output_format != 'undefined') {
    if (in_array(input.output_format,output_formats)) {
      output_format = input.output_format;
    }
  }
  
  if ((typeof input.currency != 'undefined') && (typeof input.type != 'undefined')) {
    if (in_array(input.currency,currencies) && in_array(input.type,types)) {
      var table_name = input.currency + '-' + input.type;
      var time_col_prefix = input.type.substr(0,(input.type.length - 1));
      var select_stmt = 'SELECT * FROM `' + table_name +'`';
      if ((typeof input.since != 'undefined') || (typeof input.until != 'undefined')) {
        select_stmt += ' WHERE ';
      }
      if (typeof input.since != 'undefined') {
        var since_date = new Date(input.since);
        select_stmt += '`' + time_col_prefix + '_at` >= STR_TO_DATE("' + since_date.toISOString() + '","%Y-%m-%dT%T.%fZ")';
      }
      if (typeof input.until != 'undefined') {
        var until_date = new Date(input.until);
        if (typeof input.since != 'undefined') {
          select_stmt += ' AND ';
        }
        select_stmt += '`' + time_col_prefix + '_at` <= STR_TO_DATE("' + until_date.toISOString() + '","%Y-%m-%dT%T.%fZ")';
      }
      select_stmt += ' ORDER BY `' + time_col_prefix + '_at` ASC';
  
      var lambda_params = {
        FunctionName: rds_query_function,
        InvocationType: 'RequestResponse',
        LogType: 'None',
        Payload: JSON.stringify({"query":select_stmt})
      };
      lambda.invoke(lambda_params, function(err, data) {
        if (err) {
          console.log(err, err.stack);
          callback('Lambda Invocation Error',null);
        } else {
          if (output_format === 'default') {
            if (isApiProxy) {
              request_response.body = data.Payload;
              callback(null,request_response);
            } else {
              callback(null,data.Payload);
            }
          } else if (output_format === 'highcharts') {
            var highcharts_results = {};
            var column_names = array_keys(data.Payload[0]);
            column_names.forEach(function(col,idx){
              highcharts_results[col] = [];
            });
            data.Payload.forEach(function(record,index){
              column_names.forEach(function(col,idx){
                highcharts_results[col][index] = data.Payload[index][col];
              });
            });
            if (isApiProxy) {
              request_response.body = JSON.stringify(highcharts_results);
              callback(null,request_response);
            } else {
              callback(null,highcharts_results);
            }
          }
        }
      });

    } else {
      console.log({'event':event,'context':context});
      callback('Invalid Value for either currency or type options.',null);
    }
  } else {
    console.log({'event':event,'context':context});
    callback('Must pass "currency" and "type", may optionally pass "since","until" and "output_format" ',null);
  }

};
