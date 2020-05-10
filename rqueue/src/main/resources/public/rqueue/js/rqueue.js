/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var queueName = null;
var dataPageUrl = null;
var dataKey = null;
var dataName = null;
var dataType = null;
var currentPage = 0;
var defaultPageSize = null;
var deleteAllKey = null;

function drawChartElement(data, options, div_id) {
  google.charts.load('current', {'packages': ['corechart']});
  google.charts.setOnLoadCallback(function () {
    var chart = new google.visualization.LineChart(
        document.getElementById(div_id));
    var chartData = google.visualization.arrayToDataTable(data, false);
    chart.draw(chartData, options);
  });
}

function showError(message) {
  $("#global-error-container").show();
  $("#global-error-message").text(message);
}

function ajaxRequest(url, type, payload, successHandler, failureHandler) {
  $.ajax({
    url: url,
    data: json(payload),
    type: type,
    dataType: 'json',
    contentType: 'application/json',
    processData: false,
    success: successHandler,
    fail: failureHandler
  });
}

function drawChart(payload, div_id) {
  ajaxRequest('/rqueue/api/v1/chart', 'POST', payload,
      function (response) {
        var title = response.title;
        var hTitle = response.hTitle;
        var vTitle = response.vTitle;
        var data = response.data;
        var options = {
          title: title,
          hAxis: {title: hTitle, minValue: 0, titleTextStyle: {color: '#333'}},
          vAxis: {minValue: 0, title: vTitle}
        };
        drawChartElement(data, options, div_id);
      },
      function (response) {
        console.log('failed, ' + response);
        showError("Something wet wrong! Please reload!");
      });
}

function getSelectedOption(id) {
  return $('#' + id + ' :selected').val();
}

function refreshStatsChart(chartParams, div_id) {
  var types = [];
  var aggregatorType = $('#stats-aggregator-type :selected').val();
  $.each($("input[name='data-type']:checked"), function () {
    types.push($(this).val());
  });
  chartParams["aggregationType"] = aggregatorType;
  chartParams["dateTypes"] = types;
  drawChart(chartParams, div_id);
}

function refreshLatencyChart(chartParams, div_id) {
  chartParams['aggregationType'] = $(
      '#latency-aggregator-type :selected').val();
  drawChart(chartParams, div_id);
}

function exploreData(e) {
  var element = $(e);
  dataName = element.data('name');
  dataType = element.data('type');
  dataKey = element.data('key');
}

function json(data) {
  return JSON.stringify(data);
}

function displayTable(nextOrPrev) {
  var pageSize = parseInt($('#page-size').val());
  var pageNumber = currentPage;
  if (nextOrPrev === null) {
    defaultPageSize = pageSize + '';
  } else if (nextOrPrev === true) {
    pageNumber += 1;
  } else {
    pageNumber -= 1;
  }
  var data = {
    'src': queueName,
    'count': pageSize,
    'page': pageNumber,
    'type': dataType,
    'name': dataName,
    'key': dataKey
  };
  $.ajax({
    url: dataPageUrl,
    data: data,
    traditional: true,
    success: function (response) {
      currentPage = pageNumber;
      var rows = response.rows;
      var tableHeader = $('#table-header');
      var displayPageNumberEl = $('#display-page-number');
      displayPageNumberEl.empty();
      $('#clear-queue').hide();
      for (var i = 0; i < response.actions.length; i++) {
        switch (response.actions[i]) {
          case 'DELETE':
            $('#clear-queue').show();
            break;
          default:
            console.log("Unknown action", response.actions[i])
        }
      }
      if (rows.length === pageSize) {
        $('#next-page-button').show();
      } else {
        $('#next-page-button').hide();
      }
      if (currentPage > 0) {
        $('#previous-page-button').show();
      } else {
        $('#previous-page-button').hide();
        tableHeader.empty();
        var headers = response.headers;
        for (var i = 0; i < headers.length; i++) {
          tableHeader.append("<th>" + headers[i] + "</th>");
        }
      }
      var tableBody = $('#table-body');
      tableBody.empty();
      for (var i = 0; i < rows.length; i++) {
        var tds = "";
        var row = rows[i];
        for (var j = 0; j < row.length - 1; j++) {
          tds += ("<td>" + row[j] + "</td>");
        }
        if (row[row.length - 1] === 'DELETE') {
          tds += ("<td><a href='#' class='delete-message-btn' onclick='deleteMessage(this)'>Delete </a></td>");
        } else {
          tds += ("<td>" + row[j] + "</td>");
        }
        tableBody.append("<tr>" + tds + "</tr>");
      }
      displayPageNumberEl.append("Page #", pageNumber + 1);
    },
    fail: function (response) {
      console.log('failed, ' + response);
      showError("Something wet wrong! Please reload!");
    }
  });
}

function deleteAll() {
  deleteAllKey = queueName;
  $('#delete-modal-body').empty().append(
      "Do you really want to delete <b>Queue: </b>" + queueName
      + "&nbsp;?&nbsp;This process cannot be undone.");
  $('#delete-modal').modal('show');
}

function deleteMessage(e) {
  var id = e.parentNode.parentNode.children[0].textContent;
  var url = '/rqueue/api/v1/data-set/' + queueName + "/" + id;
  $.ajax({
    url: url,
    type: 'DELETE',
    success: function (response) {
      if (response.code === 0) {
        refreshPage();
      } else {
        alert("Failed:" + response.message + " Please retry!");
        console.log(response);
      }
    },
    fail: function (response) {
      console.log('failed, ' + response);
      showError("Something went wrong! Please reload!");
    }
  });
}

function refreshPage() {
  displayTable(null);
}

function pollQueue() {
  currentPage = 0;
  refreshPage();
}

function nextPage() {
  displayTable(true);
}

function prevPage() {
  displayTable(false);
}

function updateDataType(element, callback) {
  var el = $(element);
  var key = el.val();
  var typeIdEl = $('#' + el.attr('id') + "-type");
  var url = '/rqueue/api/v1/data-type?name=' + key;
  typeIdEl.empty();
  $.ajax({
    url: url,
    success: function (response) {
      if (response.code === 0) {
        if (response.val !== 'NONE') {
          typeIdEl.text(response.val);
        }
        if (callback !== undefined) {
          callback();
        }
      } else {
        alert("Failed:" + response.message + " Please retry!");
        console.log(response);
      }
    },
    fail: function (response) {
      console.log('failed, ' + response);
      showError("Something went wrong! Please retry!");
    }
  });
}

$('#explore-queue').on('hidden.bs.modal', function () {
  currentPage = 0;
  $('#previous-page-button').hide();
  $('#next-page-button').hide();
  $('#display-page-number').empty();
  $('#page-size').val(defaultPageSize);
});

$('.delete-queue').on("click", function () {
  queueName = $(this).data('name');
  $('#delete-modal-body').empty().append(
      "Do you really want to delete <b>Queue: </b>?" + queueName
      + "&nbsp; &nbsp; This process cannot be undone.");
  $('#delete-modal').modal('show');
});

function makeQueueEmpty() {
  $.ajax({
    url: "/rqueue/api/v1/data-set/" + deleteAllKey,
    type: 'DELETE',
    success: function (response) {
      if (response.code === 0) {
        currentPage = 0;
        refreshPage();
      } else {
        alert("Failed:" + response.message + " Please retry!");
        console.log(response);
      }
    },
    fail: function (response) {
      console.log('failed, ' + response);
      showError("Something went wrong! Please reload!");
    }
  });
}

function deleteQueue() {
  $.ajax({
    url: "/rqueue/api/v1/queues/" + queueName,
    type: 'DELETE',
    success: function (response) {
      if (response.code === 0) {
        window.location.replace(window.location.href);
      } else {
        alert("Failed:" + response.message + " Please retry!");
        console.log(response);
      }
    },
    fail: function (response) {
      console.log('failed, ' + response);
      showError("Something went wrong! Please reload!");
    }
  })
}

$('.delete-btn').on("click", function () {
  if (queueName !== null) {
    deleteQueue();
  } else if (deleteAllKey !== null) {
    makeQueueEmpty();
  } else {
    console.log("Something is not correct :(");
  }
});

function enableKeyForm() {
  var dataType = $('#data-name-type').text();
  if (dataType === 'ZSET') {
    $('#data-key-form').show();
  } else {
    $('#data-key-form').hide();
  }
  dataKeyEl.val('');
  $('#view-data').data('type', dataType).data('name', dataNameEl.val());
}

function enableFormsIfRequired() {
  var dstDataType = $('#dst-data-type').text();
  var srcDataType = $('#src-data-type').text();
  debugger
  if (dstDataType === 'ZSET') {
    $('#priority-controller-form').show();
  } else if (dstDataType === '' && srcDataType !== '') {
    var dstDataName = dstDataEl.val();
    if (dstDataName !== '') {
      $('#dst-data-type-input-form').show();
    }
  }
}

function disableForms() {
  $('#priority-controller-form').hide();
  $('#dst-data-type-input-form').hide();
}

function convertVal(txt) {
  if (txt === '' || txt === undefined) {
    return null;
  }
  return txt;
}

function getSourceType() {
  return convertVal($('#src-data-type').text());
}

function getDstType() {
  var type = convertVal($('#dst-data-type').text());
  if (type !== null) {
    return type;
  }
  return convertVal(dstDataTypeInputEl.val());
}

$('#move-button').on("click", function () {
  var messageCount = $('#number-of-messages').val();
  var other = {};
  var dstType = getSourceType();
  var srcType = getDstType();
  if (srcType === null) {
    alert("Source cannot be empty.");
    return;
  }
  var payload = {
    'src': srcDataEl.val(),
    'srcType': srcType,
    'dst': dstDataEl.val(),
    'dstType': dstType
  };
  if (messageCount !== '') {
    other['messageCount'] = parseInt(messageCount);
  }
  if ($('#priority-controller-form').is(":visible")) {
    var val = $('#priority-val').val();
    if (val !== '') {
      other['newScore'] = val;
    }
    var type = getSelectedOption('priority-type');
    if (type === 'REL') {
      other['fixedScore'] = false;
    } else if (type === 'ABS') {
      other['fixedScore'] = true;
    }
  }
  payload['other'] = other;
  ajaxRequest('/rqueue/api/v1/move', 'PUT', payload,
      function (response) {
        if (response.code === 0) {
          alert(
              "Transferred " + response.numberOfMessageTransferred
              + " messages");
        } else {
          alert(response.message);
        }
      },
      function (response) {
        console.log('failed, ' + response);
        showError("Something wet wrong! Please retry!");
      });
});