/*
 * Copyright (c) 2020-2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.web.view;

import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.db.DeadLetterQueue;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerPollerView;
import com.github.sonus21.rqueue.models.registry.RqueueWorkerView;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.models.response.SubscriberRow;
import com.github.sonus21.rqueue.models.response.TerminalStorageRow;
import com.github.sonus21.rqueue.utils.DateTimeUtils;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
public class RqueueHtmlRenderer {

  // ---- Escaping helpers ----

  private static String esc(Object val) {
    if (val == null) return "";
    return val.toString()
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#x27;");
  }

  private static String jsStr(Object val) {
    if (val == null) return "";
    return val.toString()
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e");
  }

  // ---- Format helpers (mirror the Pebble custom functions) ----

  private static String fmtTime(Long millis) {
    if (millis == null || millis <= 0) return "-";
    return esc(DateTimeUtils.formatMilliToString(millis));
  }

  private static String fmtDuration(Long millis) {
    return esc(DateTimeUtils.formatMilliToCompactDuration(millis));
  }

  private static String fmtReadable(Long millis) {
    return esc(DateTimeUtils.formatMilliToReadableString(millis));
  }

  @SuppressWarnings("unchecked")
  private static String fmtDlq(List<DeadLetterQueue> queues) {
    if (CollectionUtils.isEmpty(queues)) return "";
    return esc(queues.stream().map(DeadLetterQueue::getName).collect(Collectors.joining(", ")));
  }

  private static String orDefault(String val, String fallback) {
    return (val != null && !val.isEmpty()) ? esc(val) : esc(fallback);
  }

  // ---- Model access helpers ----

  @SuppressWarnings("unchecked")
  private static <T> T get(Map<String, Object> m, String key) {
    return (T) m.get(key);
  }

  private static boolean bool(Map<String, Object> m, String key) {
    return Boolean.TRUE.equals(m.get(key));
  }

  // ---- Base layout ----

  private String base(Map<String, Object> m, String mainContent, String additionalScript) {
    String up = esc(get(m, "urlPrefix"));
    String title = m.get("title") != null ? esc(get(m, "title")) : "Rqueue Dashboard";
    return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta content="width=device-width, initial-scale=1.0" name="viewport">
          <meta content="default-src 'self' *.gstatic.com *.googleapis.com 'unsafe-inline' 'unsafe-eval'" http-equiv="Content-Security-Policy">
          <meta content='Rqueue' name="description">
          <meta content='Rqueue, task queue, scheduled queue, scheduled tasks, asynchronous processor' name="keywords">
          <title>%s</title>
          <link href="%srqueue/img/favicon.ico" rel="shortcut icon">
          <link href="%srqueue/img/apple-touch-icon.png" rel="apple-touch-icon">
          <link href="%srqueue/img/favicon-16x16.png" rel="icon" sizes="16x16" type="image/png">
          <link href="%srqueue/img/favicon-32x32.png" rel="icon" sizes="32x32" type="image/png">
          <link href="https://fonts.googleapis.com/css?family=Open+Sans:300,300i,400,400i,600,600i,700,700i" rel="stylesheet">
          <link href="%srqueue/vendor/bootstrap/css/bootstrap.min.css" rel="stylesheet">
          <link href="%srqueue/vendor/boxicons/css/boxicons.min.css" rel="stylesheet">
          <link href="%srqueue/css/rqueue.css" rel="stylesheet">
        </head>
        <body>
        <header class="fixed-top" id="header">
          <div class="container d-flex">
            <div class="logo mr-auto">
              <h1 class="text-green"><a href="%srqueue">Rqueue</a></h1>
            </div>
            %s
          </div>
        </header>
        <main id="main">
          <div class="alert alert-primary alert-dismissible fade display-none" id="global-error-container" role="alert">
            <button aria-label="Close" class="close" data-dismiss="alert" type="button">
              <span aria-hidden="true">&times;</span>
            </button>
            <p id="global-error-message"></p>
          </div>
          %s
        </main>
        %s
        <div class="modal fade" id="delete-modal">
          <div class="modal-dialog modal-confirm">
            <div class="modal-content">
              <div class="modal-header">
                <h4 class="modal-title" id="delete-modal-title">Are you sure?</h4>
                <button aria-hidden="true" class="close" data-dismiss="modal" type="button">&times;</button>
              </div>
              <div class="modal-body">
                <p id="delete-modal-body">Do you really want to delete this? This process cannot be undone.</p>
              </div>
              <div class="modal-footer">
                <button class="btn btn-info" data-dismiss="modal" type="button">Cancel</button>
                <button class="btn btn-danger delete-btn" type="button">Delete</button>
              </div>
            </div>
          </div>
        </div>
        <div class="modal fade" id="info-modal">
          <div class="modal-dialog modal-info">
            <div class="modal-content">
              <div class="modal-header">
                <h4 class="modal-title" id="info-modal-title">Title</h4>
                <button aria-hidden="true" class="close" data-dismiss="modal" type="button">&times;</button>
              </div>
              <div class="modal-body">
                <p id="info-modal-body">Some text</p>
              </div>
              <div class="modal-footer">
                <button class="btn btn-info" data-dismiss="modal" type="button">Ok</button>
              </div>
            </div>
          </div>
        </div>
        <script src="https://www.gstatic.com/charts/loader.js" type="application/javascript"></script>
        <script src="%srqueue/vendor/jquery/jquery.min.js" type="application/javascript"></script>
        <script src="%srqueue/vendor/bootstrap/js/bootstrap.bundle.min.js" type="application/javascript"></script>
        <script src="%srqueue/js/rqueue.js" type="application/javascript"></script>
        <script type="application/javascript">urlPrefix = "%s";</script>
        %s
        </body>
        </html>
        """.formatted(
        title,
        up, up, up, up,     // favicons x4
        up, up, up,          // css x3
        up,                  // logo href
        navBar(m),
        mainContent,
        footerHtml(m),
        up, up, up,          // scripts x3
        jsStr(get(m, "urlPrefix")),
        additionalScript
    );
  }

  private String navBar(Map<String, Object> m) {
    String up = esc(get(m, "urlPrefix"));
    boolean hideRunning = bool(m, "hideRunningPanel");
    boolean hideScheduled = bool(m, "hideScheduledPanel");
    StringBuilder sb = new StringBuilder("<nav class=\"nav-menu display-none d-lg-block\"><ul>");
    sb.append(navItem(bool(m, "queuesActive"), up, "queues", "bx-coin-stack", "Queues"));
    sb.append(navItem(bool(m, "workersActive"), up, "workers", "bx-user-pin", "Workers"));
    if (!hideRunning) {
      sb.append(navItem(bool(m, "runningActive"), up, "running", "bx-sync", "Running"));
    }
    if (!hideScheduled) {
      sb.append(navItem(bool(m, "scheduledActive"), up, "scheduled", "bxs-hourglass", "Scheduled"));
    }
    sb.append(navItem(bool(m, "pendingActive"), up, "pending", "bx-spreadsheet", "Pending"));
    sb.append(navItem(bool(m, "deadActive"), up, "dead", "bx-ghost", "Dead"));
    sb.append(navItem(bool(m, "utilityActive"), up, "utility", "bxs-wrench", "Utility"));
    sb.append("</ul></nav>");
    return sb.toString();
  }

  private String navItem(boolean active, String urlPrefix, String path, String icon, String label) {
    return "<li class=\"" + (active ? "active" : "") + "\">"
        + "<a href=\"" + urlPrefix + "rqueue/" + path + "\">"
        + "<i class='bx " + icon + "'></i><span>" + label + "</span></a></li>";
  }

  private String footerHtml(Map<String, Object> m) {
    return """
        <footer id="footer">
          <div class="container">
            <div class="row">
              <ul class="footer-links">
                <li><h3><a href="https://github.com/sonus21/rqueue" target="_blank">Rqueue</a></h3></li>
                <li><a href="#">Version:&nbsp;<p class="text-white float-right">%s</p></a></li>
                <li><a href="%s" target="_blank">Latest Version:&nbsp;<p class="text-white float-right">%s</p></a></li>
                <li><a href="#">Time:&nbsp;<p class="text-white float-right">%s (%sMs)</p></a></li>
              </ul>
            </div>
          </div>
        </footer>
        """.formatted(
        esc(get(m, "version")),
        esc(get(m, "releaseLink")),
        esc(get(m, "latestVersion")),
        esc(get(m, "time")),
        esc(get(m, "timeInMilli"))
    );
  }

  // ---- Shared partials ----

  private String statsChartPartial(Map<String, Object> m) {
    List<ChartDataType> typeSelectors = get(m, "typeSelectors");
    List<AggregationType> aggTypes = get(m, "aggregatorTypes");
    DataSelectorResponse aggDateCounter = get(m, "aggregatorDateCounter");

    StringBuilder typeCheckboxes = new StringBuilder();
    if (typeSelectors != null) {
      for (ChartDataType t : typeSelectors) {
        typeCheckboxes.append("""
            <div class="form-check">
              <input checked="checked" class="form-check-input" id="%s" name="data-type" type="checkbox" value="%s">
              <label class="form-check-label" for="%s">%s</label>
            </div>
            """.formatted(esc(t.name()), esc(t.name()), esc(t.name()), esc(t.getDescription())));
      }
    }

    StringBuilder aggTypeOptions = new StringBuilder();
    if (aggTypes != null) {
      boolean first = true;
      for (AggregationType a : aggTypes) {
        aggTypeOptions.append("<option").append(first ? " selected" : "").append(">")
            .append(esc(a.toString())).append("</option>");
        first = false;
      }
    }

    String counterTitle = aggDateCounter != null ? esc(aggDateCounter.getTitle()) : "";
    StringBuilder counterOptions = new StringBuilder();
    if (aggDateCounter != null && aggDateCounter.getData() != null) {
      for (Pair<String, String> p : aggDateCounter.getData()) {
        counterOptions.append("<option value=\"").append(esc(p.getFirst())).append("\">")
            .append(esc(p.getSecond())).append("</option>");
      }
    }

    return """
        <h2 class="text-center">Message Stats</h2>
        <div id="stats_chart"></div>
        <div class="container">
          <div class="row">
            <div class="col-md-6 offset-md-3">
              <div class="row dashboard-chart-form">
                <div class="type_selectors col-md-7">
                  <p>Select Data Type</p>
                  %s
                </div>
                <div class="dashboard-form-action col-md-5">
                  <div class="form-group">
                    <label for="stats-aggregator-type">Select Aggregator Type</label>
                    <select class="form-control" id="stats-aggregator-type" name="aggregator-type">%s</select>
                  </div>
                  <div class="form-group">
                    <label for="stats-nday">%s</label>
                    <select class="form-control" id="stats-nday" name="aggregator-day-count">%s</select>
                  </div>
                  <div class="form-group">
                    <button class="btn btn-success" id="refresh-chart">Display</button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        """.formatted(typeCheckboxes, aggTypeOptions, counterTitle, counterOptions);
  }

  private String latencyChartPartial(Map<String, Object> m) {
    List<AggregationType> aggTypes = get(m, "aggregatorTypes");
    DataSelectorResponse aggDateCounter = get(m, "aggregatorDateCounter");

    StringBuilder aggTypeOptions = new StringBuilder();
    if (aggTypes != null) {
      boolean first = true;
      for (AggregationType a : aggTypes) {
        aggTypeOptions.append("<option").append(first ? " selected" : "").append(">")
            .append(esc(a.toString())).append("</option>");
        first = false;
      }
    }

    String counterTitle = aggDateCounter != null ? esc(aggDateCounter.getTitle()) : "";
    StringBuilder counterOptions = new StringBuilder();
    if (aggDateCounter != null && aggDateCounter.getData() != null) {
      for (Pair<String, String> p : aggDateCounter.getData()) {
        counterOptions.append("<option value=\"").append(esc(p.getFirst())).append("\">")
            .append(esc(p.getSecond())).append("</option>");
      }
    }

    return """
        <h2 class="text-center">Latency Graph!</h2>
        <div id="latency_chart"></div>
        <div class="container">
          <div class="row">
            <div class="col-md-6 offset-md-3">
              <div class="dashboard-chart-form latency-chart-form">
                <div class="row">
                  <div class="col-md-6">
                    <div class="form-group">
                      <label for="latency-aggregator-type">Select Aggregator Type</label>
                      <select class="form-control" id="latency-aggregator-type" name="latency-aggregator-type">%s</select>
                    </div>
                  </div>
                  <div class="col-md-6">
                    <div class="form-group">
                      <label for="latency-nday">%s</label>
                      <select class="form-control" id="latency-nday" name="latency-day-count">%s</select>
                    </div>
                  </div>
                </div>
                <div class="row">
                  <div class="col-md-2 offset-md-5">
                    <div class="form-group">
                      <button class="btn btn-success" id="refresh-latency-chart">Display</button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        """.formatted(aggTypeOptions, counterTitle, counterOptions);
  }

  private String dataExplorerModalPartial(Map<String, Object> m) {
    String queueName = m.get("queueName") != null ? esc(get(m, "queueName")) : "";
    return """
        <div class="modal fade" id="explore-queue">
          <div class="modal-dialog modal-data-explorer modal-lg modal-dialog-centered" role="document">
            <div class="modal-content explorer-modal-content">
              <div class="modal-header explorer-modal-header">
                <div class="explorer-modal-heading">
                  <span class="explorer-modal-kicker">Queue Explorer</span>
                  <h4 class="modal-title" id="explorer-title">Queue: <b>%s</b></h4>
                </div>
                <button aria-label="Close" class="close explorer-modal-close" data-dismiss="modal" type="button">&times;</button>
              </div>
              <div class="modal-body explorer-modal-body">
                <div class="explorer-toolbar">
                  <div class="explorer-toolbar-left">
                    <label class="explorer-select-group" for="page-size">
                      <span class="explorer-control-label">Rows</span>
                      <select id="page-size">
                        <option selected value="10">10</option>
                        <option value="25">25</option>
                        <option value="50">50</option>
                        <option value="100">100</option>
                      </select>
                    </label>
                  </div>
                  <div class="explorer-toolbar-actions">
                    <button class="btn btn-danger btn-delete-all btn-sm display-none" id="clear-queue" type="button">Delete All</button>
                    <button class="btn btn-info btn-sm btn-poll" id="poll-queue-btn" type="button">Refresh</button>
                  </div>
                </div>
                <div class="explorer-table-shell">
                  <div class="table-responsive explorer-table-wrap" id="explore-data-table">
                    <table class="table table-bordered explorer-table">
                      <thead><tr id="table-header"><th></th></tr></thead>
                      <tbody id="table-body"></tbody>
                    </table>
                  </div>
                </div>
              </div>
              <div class="modal-footer data-explorer-footer explorer-modal-footer">
                <div class="explorer-pagination">
                  <button class="btn btn-outline-secondary" id="previous-page-button" type="button">Previous</button>
                  <span class="explorer-page-indicator" id="display-page-number"></span>
                  <button class="btn btn-info" id="next-page-button" type="button">Next</button>
                </div>
              </div>
            </div>
          </div>
        </div>
        """.formatted(queueName);
  }

  private String paginationControls(boolean hasPrev, boolean hasNext, int prevPage, int nextPage, int currentPage, int totalPages) {
    StringBuilder sb = new StringBuilder();
    sb.append("<div class=\"worker-pagination\">");
    if (hasPrev) {
      sb.append("<a class=\"worker-page-btn\" href=\"?page=").append(prevPage).append("\">Previous</a>");
    }
    sb.append("<span class=\"worker-page-pill\">Page ").append(currentPage).append("</span>");
    if (hasNext) {
      sb.append("<a class=\"worker-page-btn worker-page-btn-primary\" href=\"?page=").append(nextPage).append("\">Next</a>");
    }
    sb.append("</div>");
    return sb.toString();
  }

  // ---- Page renderers ----

  public String renderIndex(Map<String, Object> m) {
    String main = statsChartPartial(m) + "<hr/>" + latencyChartPartial(m);
    String script = """
        <script type="application/javascript">
          var chartParams = {'type': 'STATS', 'aggregationType': 'DAILY'};
          var latencyChartParams = {'type': 'LATENCY', 'aggregationType': 'DAILY'};
          $(document).ready(function () {
            drawChart(chartParams, "stats_chart");
            drawChart(latencyChartParams, "latency_chart");
            $('#refresh-chart').click(function () { refreshStatsChart(chartParams, "stats_chart"); });
            $('#refresh-latency-chart').click(function () { refreshLatencyChart(latencyChartParams, "latency_chart"); });
            attachChartEventListeners();
          });
        </script>
        """;
    return base(m, main, script);
  }

  public String renderQueues(Map<String, Object> m) {
    List<QueueConfig> queues = get(m, "queues");
    List<Entry<String, List<Entry<NavTab, RedisDataDetail>>>> queueConfigs = get(m, "queueConfigs");
    int currentPage = ((Number) m.get("currentPage")).intValue();
    int totalPages = ((Number) m.get("totalPages")).intValue();
    boolean hasPrev = bool(m, "hasPreviousPage");
    boolean hasNext = bool(m, "hasNextPage");
    int prevPage = ((Number) m.get("previousPage")).intValue();
    int nextPage = ((Number) m.get("nextPage")).intValue();
    int totalQueueCount = ((Number) m.get("totalQueueCount")).intValue();
    String storageKicker = orDefault(get(m, "storageKicker"), "Redis");
    String storageDesc = esc(get(m, "storageDescription"));
    String pagination = paginationControls(hasPrev, hasNext, prevPage, nextPage, currentPage, totalPages);

    StringBuilder cards = new StringBuilder();
    if (queues == null || queues.isEmpty()) {
      cards.append("""
          <section class="queue-empty-state" role="alert">
            <h2>No queues available.</h2>
            <p>Queue definitions will appear here after Rqueue loads listener metadata.</p>
          </section>
          """);
    } else {
      cards.append("<section class=\"queue-card-grid\">");
      for (QueueConfig meta : queues) {
        boolean paused = meta.isPaused();
        String name = esc(meta.getName());
        String stateLabel = paused ? "Paused" : "Live";
        String stateCss = paused ? "queue-state-paused" : "queue-state-live";
        String stateIcon = paused ? "bx-pause-circle" : "bx-check-circle";
        String pauseIcon = paused ? "bx-play-circle" : "bx-pause-circle";
        String pauseTitle = paused ? "Unpause" : "Pause";
        boolean unboundedConc = meta.getConcurrency().getMin() == -1 && meta.getConcurrency().getMax() == -1;
        String concurrency = unboundedConc
            ? "<span aria-label=\"Unbounded concurrency\" class=\"queue-metric-infinity\" data-placement=\"top\" data-toggle=\"tooltip\" title=\"Unbounded concurrency\"><i class=\"bx bx-infinite\"></i></span>"
            : esc(meta.getConcurrency().getMin()) + " to " + esc(meta.getConcurrency().getMax());
        String retries = meta.isUnlimitedRetry()
            ? "<span aria-label=\"Unlimited retries\" class=\"queue-metric-infinity\" data-placement=\"top\" data-toggle=\"tooltip\" title=\"Unlimited retries\"><i class=\"bx bx-infinite\"></i></span>"
            : esc(meta.getNumRetry());

        cards.append("""
            <article class="queue-card">
              <div class="queue-card-top">
                <div class="queue-card-heading">
                  <span class="queue-card-label">Queue</span>
                  <h2 class="queue-card-title"><a href="queues/%s">%s</a></h2>
                </div>
                <div class="queue-card-actions">
                  <span aria-label="%s" class="queue-state-badge %s" data-placement="top" data-toggle="tooltip" title="%s">
                    <i class="bx %s"></i>
                  </span>
                  <button class="queue-pause-toggle" type="button">
                    <i class="bx pause-queue-btn %s" data-placement="top" data-queue="%s" data-toggle="tooltip" title="%s"></i>
                  </button>
                </div>
              </div>
              <div class="queue-card-metrics">
                <div class="queue-metric">
                  <span aria-label="Concurrency" class="queue-metric-label queue-metric-icon" data-placement="top" data-toggle="tooltip" title="Concurrency"><i class="bx bx-sitemap"></i></span>
                  <strong class="queue-metric-value">%s</strong>
                </div>
                <div class="queue-metric">
                  <span aria-label="Retry Count" class="queue-metric-label queue-metric-icon" data-placement="top" data-toggle="tooltip" title="Retry Count"><i class="bx bx-refresh"></i></span>
                  <strong class="queue-metric-value">%s</strong>
                </div>
                <div class="queue-metric">
                  <span aria-label="Visibility Timeout" class="queue-metric-label queue-metric-icon" data-placement="top" data-toggle="tooltip" title="Visibility Timeout"><i class="bx bx-time-five"></i></span>
                  <strong class="queue-metric-value">%s</strong>
                </div>
              </div>
              <div class="queue-card-meta">
                <div class="queue-meta-block">
                  <span class="queue-meta-label">Dead Letter Queue(s)</span>
                  <div class="queue-meta-value queue-dlq-list">%s</div>
                </div>
                <div class="queue-meta-grid">
                  <div class="queue-meta-block">
                    <span class="queue-meta-label">Created</span>
                    <div class="queue-meta-value queue-meta-value-inline">%s</div>
                  </div>
                  <div class="queue-meta-block">
                    <span class="queue-meta-label">Updated</span>
                    <div class="queue-meta-value queue-meta-value-inline">%s</div>
                  </div>
                </div>
              </div>
            </article>
            """.formatted(
            name, name,
            stateLabel, stateCss, stateLabel, stateIcon,
            pauseIcon, name, pauseTitle,
            concurrency, retries,
            fmtDuration(meta.getVisibilityTimeout()),
            fmtDlq(meta.getDeadLetterQueues()),
            fmtReadable(meta.getCreatedOn()),
            fmtReadable(meta.getUpdatedOn())
        ));
      }
      cards.append("</section>");
    }

    // Storage footprint section
    StringBuilder storageGrid = new StringBuilder();
    if (queueConfigs != null) {
      for (Entry<String, List<Entry<NavTab, RedisDataDetail>>> config : queueConfigs) {
        StringBuilder rows = new StringBuilder();
        for (Entry<NavTab, RedisDataDetail> meta : config.getValue()) {
          String sizeHtml = meta.getValue().getSize() < 0
              ? "Queue-backed"
              : esc(meta.getValue().getSize());
          rows.append("<tr><td>").append(esc(meta.getKey())).append("</td>")
              .append("<td>").append(esc(meta.getValue().getName())).append("</td>")
              .append("<td>").append(sizeHtml).append("</td></tr>");
        }
        storageGrid.append("""
            <article class="queue-storage-card">
              <div class="queue-storage-card-head">
                <span class="queue-storage-label">Queue</span>
                <h3 class="queue-storage-title">%s</h3>
              </div>
              <div class="table-responsive">
                <table class="table queue-storage-table">
                  <thead><tr><th>Type</th><th>Name</th><th>Size</th></tr></thead>
                  <tbody>%s</tbody>
                </table>
              </div>
            </article>
            """.formatted(esc(config.getKey()), rows));
      }
    }

    String toolbarNote = totalQueueCount > 0
        ? "Showing queue records for this page."
        : "No queues are registered.";

    String main = """
        <div class="container queue-dashboard">
          <section class="queue-hero">
            <div class="queue-hero-copy">
              <span class="queue-hero-kicker">Queue Catalog</span>
              <h1 class="queue-hero-title">Operational View of Every Queue</h1>
              <p class="queue-hero-subtitle">Browse queue configuration, retry policy, pause state, and backing %s structures from a single page.</p>
            </div>
            <div class="queue-hero-meta">
              <div class="queue-hero-stat">
                <span class="queue-hero-stat-label">Queues</span>
                <strong class="queue-hero-stat-value">%d</strong>
              </div>
              <div class="queue-hero-stat">
                <span class="queue-hero-stat-label">Page</span>
                <strong class="queue-hero-stat-value">%d / %d</strong>
              </div>
            </div>
          </section>
          <section class="queue-toolbar">
            <div class="queue-toolbar-note">%s</div>
            %s
          </section>
          %s
          <section class="queue-storage-section">
            <div class="queue-section-header">
              <div>
                <span class="queue-section-kicker">%s Layout</span>
                <h2 class="queue-section-title">Queue Storage Footprint</h2>
              </div>
              <p class="queue-section-copy">%s</p>
            </div>
            <div class="queue-storage-grid">%s</div>
          </section>
          <section class="queue-toolbar queue-toolbar-bottom">
            <div class="queue-toolbar-note">Use each queue card to open detailed queue state and message explorers.</div>
            %s
          </section>
        </div>
        """.formatted(
        storageKicker, totalQueueCount, currentPage, totalPages,
        toolbarNote, pagination,
        cards,
        storageKicker, storageDesc, storageGrid,
        paginationControls(hasPrev, hasNext, prevPage, nextPage, currentPage, totalPages)
    );
    return base(m, main, "");
  }

  public String renderWorkers(Map<String, Object> m) {
    List<RqueueWorkerView> workers = get(m, "workers");
    int currentPage = ((Number) m.get("currentPage")).intValue();
    int totalPages = ((Number) m.get("totalPages")).intValue();
    boolean hasPrev = bool(m, "hasPreviousPage");
    boolean hasNext = bool(m, "hasNextPage");
    int prevPage = ((Number) m.get("previousPage")).intValue();
    int nextPage = ((Number) m.get("nextPage")).intValue();
    int totalWorkerCount = ((Number) m.get("totalWorkerCount")).intValue();
    String pagination = paginationControls(hasPrev, hasNext, prevPage, nextPage, currentPage, totalPages);

    String toolbarNote = totalWorkerCount > 0
        ? "Showing worker records for this page."
        : "Waiting for worker heartbeats.";

    StringBuilder workerList = new StringBuilder();
    if (workers == null || workers.isEmpty()) {
      workerList.append("""
          <section class="worker-empty-state" role="alert">
            <h2>No worker heartbeat is available yet.</h2>
            <p>The page will populate after a worker starts polling queues and reports registry metadata.</p>
          </section>
          """);
    } else {
      workerList.append("<section class=\"worker-list\">");
      boolean first = true;
      for (RqueueWorkerView worker : workers) {
        StringBuilder pollers = new StringBuilder();
        for (RqueueWorkerPollerView poller : worker.getPollers()) {
          String statusCss = "ACTIVE".equals(poller.getStatus()) ? "worker-status-active"
              : "STALE".equals(poller.getStatus()) ? "worker-status-stale"
              : "PAUSED".equals(poller.getStatus()) ? "worker-status-paused" : "";
          String lastPollTime = fmtTime(poller.getLastPollAt());
          String lastMsgTime = fmtTime(poller.getLastMessageAt());
          String lastExhTime = fmtTime(poller.getLastCapacityExhaustedAt());
          String consumerNameHtml = poller.getConsumerName() != null
              ? "<span class=\"worker-consumer-name\">" + esc(poller.getConsumerName()) + "</span>" : "";
          pollers.append("""
              <article class="worker-queue-card">
                <div class="worker-queue-card-head">
                  <div>
                    <span class="worker-queue-label">Queue</span>
                    <h3 class="worker-queue-title"><a href="queues/%s">%s</a></h3>
                    %s
                  </div>
                  <span class="worker-status-badge %s">%s</span>
                </div>
                <div class="worker-queue-timeline">
                  <div class="worker-queue-event">
                    <span class="worker-queue-event-label">Last Poll</span>
                    <strong class="worker-queue-event-time">%s</strong>
                    <span class="worker-queue-event-age">%s</span>
                  </div>
                  <div class="worker-queue-event">
                    <span class="worker-queue-event-label">Last Message</span>
                    <strong class="worker-queue-event-time">%s</strong>
                    <span class="worker-queue-event-age">%s</span>
                  </div>
                  <div class="worker-queue-event">
                    <span class="worker-queue-event-label">Last Exhausted</span>
                    <strong class="worker-queue-event-time">%s</strong>
                    <span class="worker-queue-event-age">%s</span>
                  </div>
                </div>
                <div class="worker-queue-footer">
                  <span class="worker-queue-footer-label">Exhausted Count</span>
                  <strong class="worker-queue-footer-value">%s</strong>
                </div>
              </article>
              """.formatted(
              esc(poller.getQueue()), esc(poller.getQueue()),
              consumerNameHtml,
              statusCss, esc(poller.getStatus()),
              lastPollTime, esc(poller.getLastPollAge()),
              lastMsgTime, esc(poller.getLastMessageAge()),
              lastExhTime, esc(poller.getLastCapacityExhaustedAge()),
              esc(poller.getCapacityExhaustedCount())
          ));
        }
        workerList.append("""
            <details class="worker-panel" %s>
              <summary class="worker-panel-summary">
                <div class="worker-panel-main">
                  <div class="worker-panel-title-row">
                    <span class="worker-id-pill">Worker</span>
                    <h2 class="worker-panel-title">%s</h2>
                  </div>
                  <div class="worker-panel-meta">
                    <span class="worker-meta-chip">Host %s</span>
                    <span class="worker-meta-chip">PID %s</span>
                    <span class="worker-meta-chip">Last Poll %s</span>
                    <span class="worker-meta-chip">%s</span>
                  </div>
                </div>
                <div class="worker-panel-stats">
                  <div class="worker-stat-chip"><span class="worker-stat-chip-label">Active</span><strong>%s</strong></div>
                  <div class="worker-stat-chip worker-stat-chip-warning"><span class="worker-stat-chip-label">Stale</span><strong>%s</strong></div>
                  <div class="worker-stat-chip worker-stat-chip-danger"><span class="worker-stat-chip-label">Recent Exhaustion</span><strong>%s</strong></div>
                </div>
                <span class="worker-panel-caret" aria-hidden="true"></span>
              </summary>
              <div class="worker-panel-body">
                <div class="worker-queue-grid">%s</div>
              </div>
            </details>
            """.formatted(
            first ? "open" : "",
            esc(worker.getWorkerId()),
            esc(worker.getHost()), esc(worker.getPid()),
            fmtTime(worker.getLastPollAt()), esc(worker.getLastPollAge()),
            esc(worker.getActiveQueues()), esc(worker.getStaleQueues()),
            esc(worker.getRecentCapacityExhaustedQueues()),
            pollers
        ));
        first = false;
      }
      workerList.append("</section>");
    }

    String main = """
        <div class="container worker-dashboard">
          <section class="worker-hero">
            <div class="worker-hero-copy">
              <span class="worker-hero-kicker">Worker Registry</span>
              <h1 class="worker-hero-title">Live Pollers Across Your Fleet</h1>
              <p class="worker-hero-subtitle">Inspect poll activity, queue ownership, and recent capacity pressure without leaving the dashboard.</p>
            </div>
            <div class="worker-hero-meta">
              <div class="worker-hero-stat">
                <span class="worker-hero-stat-label">Workers</span>
                <strong class="worker-hero-stat-value">%d</strong>
              </div>
              <div class="worker-hero-stat">
                <span class="worker-hero-stat-label">Page</span>
                <strong class="worker-hero-stat-value">%d / %d</strong>
              </div>
            </div>
          </section>
          <section class="worker-toolbar">
            <div class="worker-toolbar-note">%s</div>
            %s
          </section>
          %s
          <section class="worker-toolbar worker-toolbar-bottom">
            <div class="worker-toolbar-note">Use the queue cards to drill into the queue detail page.</div>
            %s
          </section>
        </div>
        """.formatted(
        totalWorkerCount, currentPage, totalPages,
        toolbarNote, pagination,
        workerList,
        paginationControls(hasPrev, hasNext, prevPage, nextPage, currentPage, totalPages)
    );
    return base(m, main, "");
  }

  public String renderQueueDetail(Map<String, Object> m) {
    String queueName = esc(get(m, "queueName"));
    QueueConfig config = get(m, "config");
    List<SubscriberRow> subscribers = get(m, "subscribers");
    if (subscribers == null) subscribers = List.of();
    List<TerminalStorageRow> terminalRows = get(m, "terminalRows");

    // Header
    boolean paused = config != null && config.isPaused();
    String configName = config != null ? esc(config.getName()) : queueName;
    String stateLabel = paused ? "Paused" : "Live";
    String stateCss = paused ? "qd-state-paused" : "qd-state-live";
    String stateIcon = paused ? "bx-pause-circle" : "bx-check-circle";

    String pauseBtn = "";
    if (config != null) {
      String pauseIcon = paused ? "bx-play-circle" : "bx-pause-circle";
      String pauseTitle = paused ? "Unpause" : "Pause";
      pauseBtn = """
          <button class="qd-pause-btn" type="button" title="%s queue">
            <i class="bx pause-queue-btn %s" data-queue="%s" data-placement="top" data-toggle="tooltip" title="%s"></i>
          </button>
          """.formatted(pauseTitle, pauseIcon, configName, pauseTitle);
    }

    long totalInFlight = subscribers.stream().mapToLong(SubscriberRow::getInFlight).sum();
    long firstPending = subscribers.isEmpty() ? 0 : subscribers.get(0).getPending();

    // Config chip strip
    String configStrip = "";
    if (config != null) {
      boolean unboundedConc = config.getConcurrency().getMin() == -1 && config.getConcurrency().getMax() == -1;
      String concHtml = unboundedConc
          ? "<i class=\"bx bx-infinite\"></i> Unbounded"
          : esc(config.getConcurrency().getMin()) + "–" + esc(config.getConcurrency().getMax());
      String retryHtml = config.isUnlimitedRetry()
          ? "<i class=\"bx bx-infinite\"></i> Unlimited"
          : esc(config.getNumRetry());
      String dlqHtml = config.getDeadLetterQueues() == null || config.getDeadLetterQueues().isEmpty()
          ? "<span class=\"qd-muted\">—</span>"
          : fmtDlq(config.getDeadLetterQueues());

      configStrip = """
          <div class="qd-config">
            <div class="qd-config-cell"><span class="qd-config-label"><i class="bx bx-sitemap"></i> Concurrency</span><span class="qd-config-value">%s</span></div>
            <div class="qd-config-cell"><span class="qd-config-label"><i class="bx bx-refresh"></i> Retries</span><span class="qd-config-value">%s</span></div>
            <div class="qd-config-cell"><span class="qd-config-label"><i class="bx bx-time-five"></i> Visibility</span><span class="qd-config-value">%s</span></div>
            <div class="qd-config-cell"><span class="qd-config-label"><i class="bx bx-skull"></i> DLQ</span><span class="qd-config-value">%s</span></div>
            <div class="qd-config-cell qd-config-cell-meta"><span class="qd-config-label">Created</span><span class="qd-config-value">%s</span></div>
            <div class="qd-config-cell qd-config-cell-meta"><span class="qd-config-label">Updated</span><span class="qd-config-value">%s</span></div>
          </div>
          """.formatted(
          concHtml, retryHtml,
          fmtDuration(config.getVisibilityTimeout()),
          dlqHtml,
          fmtTime(config.getCreatedOn()),
          fmtTime(config.getUpdatedOn())
      );
    }

    // Subscribers table
    StringBuilder subRows = new StringBuilder();
    for (SubscriberRow sub : subscribers) {
      String typeLabel = orDefault(sub.getTypeLabel(), sub.getDataType() != null ? sub.getDataType().toString() : "");
      String statusHtml = sub.getStatus() != null
          ? "<span class=\"qd-status qd-status-" + sub.getStatus().toLowerCase() + "\">" + esc(sub.getStatus()) + "</span>"
          : "<span class=\"qd-muted\">—</span>";
      String hostHtml = sub.getHost() != null
          ? esc(sub.getHost()) + (sub.getPid() != null ? " <small class=\"qd-muted\">/ " + esc(sub.getPid()) + "</small>" : "")
          : "<span class=\"qd-muted\">—</span>";
      String pollHtml = sub.getLastPollAt() > 0
          ? "<div class=\"qd-poll-time\">" + fmtTime(sub.getLastPollAt()) + "</div>"
          + (sub.getLastPollAge() != null ? "<small class=\"qd-muted\">" + esc(sub.getLastPollAge()) + " ago</small>" : "")
          : "<span class=\"qd-muted\">—</span>";
      String workerCountHtml = sub.getWorkerCount() > 0
          ? "<strong>" + sub.getWorkerCount() + "</strong>"
          : "<span class=\"qd-muted\">—</span>";
      String pendingSharedHtml = sub.isPendingShared()
          ? "<small class=\"qd-muted\">shared</small>" : "";
      String dataTypeName = sub.getDataType() != null ? esc(sub.getDataType().toString()) : "";
      subRows.append("""
          <tr>
            <td><a class="qd-link data-explorer" data-name="%s" data-consumer="%s" data-target="#explore-queue" data-toggle="modal" data-type="%s" data-type-label="%s" href="#">%s</a></td>
            <td><span class="qd-pill">%s</span></td>
            <td><code class="qd-code">%s</code></td>
            <td class="qd-num"><strong>%s</strong> %s</td>
            <td class="qd-num"><strong>%s</strong></td>
            <td class="qd-num">%s</td>
            <td>%s</td>
            <td>%s</td>
            <td>%s</td>
          </tr>
          """.formatted(
          esc(sub.getStorageName()), esc(sub.getConsumerName()),
          dataTypeName, typeLabel,
          esc(sub.getConsumerName()),
          typeLabel,
          esc(sub.getStorageName()),
          sub.getPending(), pendingSharedHtml,
          sub.getInFlight(),
          workerCountHtml,
          statusHtml, hostHtml, pollHtml
      ));
    }

    String subscribersSection;
    if (subscribers.isEmpty()) {
      subscribersSection = "<div class=\"qd-empty\">No subscribers attached yet.</div>";
    } else {
      subscribersSection = """
          <table class="qd-table">
            <thead>
              <tr>
                <th>Consumer</th><th>Type</th><th>Storage</th>
                <th class="qd-num">Pending</th><th class="qd-num">In-Flight</th>
                <th class="qd-num">Workers</th><th>Status</th><th>Host</th><th>Last Poll</th>
              </tr>
            </thead>
            <tbody>%s</tbody>
          </table>
          """.formatted(subRows);
    }

    // Terminal storage table
    String terminalSection = "";
    if (!CollectionUtils.isEmpty(terminalRows)) {
      StringBuilder termRows = new StringBuilder();
      for (TerminalStorageRow row : terminalRows) {
        String rowTypeLabel = orDefault(row.getTypeLabel(), row.getDataType() != null ? row.getDataType().toString() : "");
        String sizeHtml = row.getSize() < 0
            ? "<span class=\"qd-muted\">Queue-backed</span>"
            : (row.isApproximate() ? "<span class=\"qd-muted\">~</span>" : "") + "<strong>" + row.getSize() + "</strong>";
        String tabName = row.getTab() != null ? row.getTab().name() : "";
        String dataTypeName = row.getDataType() != null ? esc(row.getDataType().toString()) : "";
        termRows.append("""
            <tr>
              <td><span class="qd-bucket qd-bucket-%s">%s</span></td>
              <td><span class="qd-pill">%s</span></td>
              <td><a class="qd-link data-explorer" data-name="%s" data-target="#explore-queue" data-toggle="modal" data-type="%s" data-type-label="%s" href="#"><code class="qd-code">%s</code></a></td>
              <td class="qd-num">%s</td>
            </tr>
            """.formatted(
            esc(tabName.toLowerCase()), esc(tabName),
            rowTypeLabel,
            esc(row.getStorageName()), dataTypeName, rowTypeLabel,
            esc(row.getStorageName()),
            sizeHtml
        ));
      }
      terminalSection = """
          <section class="qd-section">
            <div class="qd-section-head">
              <h2 class="qd-section-title">Terminal Storage <span class="qd-count">%d</span></h2>
              <span class="qd-section-hint">Shared buckets — completed and dead-letter messages.</span>
            </div>
            <table class="qd-table">
              <thead><tr><th>Bucket</th><th>Type</th><th>Storage</th><th class="qd-num">Size</th></tr></thead>
              <tbody>%s</tbody>
            </table>
          </section>
          """.formatted(terminalRows.size(), termRows);
    }

    String main = """
        <div class="container qd">
          <header class="qd-header">
            <div class="qd-header-title">
              <h1 class="qd-name">%s</h1>
              <span class="qd-state %s"><i class="bx %s"></i> %s</span>
              %s
            </div>
            <div class="qd-header-stats">
              <span class="qd-stat"><strong>%d</strong> subscribers</span>
              <span class="qd-stat-sep">·</span>
              <span class="qd-stat"><strong>%d</strong> pending</span>
              <span class="qd-stat-sep">·</span>
              <span class="qd-stat"><strong>%d</strong> in-flight</span>
            </div>
          </header>
          %s
          <section class="qd-section">
            <div class="qd-section-head">
              <h2 class="qd-section-title">Subscribers <span class="qd-count">%d</span></h2>
              <span class="qd-section-hint">Click a consumer to browse its messages.</span>
            </div>
            %s
          </section>
          %s
          <details class="qd-charts" id="queue-detail-charts">
            <summary class="qd-charts-head">
              <span class="qd-section-title">Stats &amp; Latency</span>
              <span class="qd-section-hint">Click to expand</span>
            </summary>
            <div class="qd-charts-body">
              %s
              <hr/>
              %s
            </div>
          </details>
          %s
        </div>
        """.formatted(
        configName,
        stateCss, stateIcon, stateLabel,
        pauseBtn,
        subscribers.size(), firstPending, totalInFlight,
        configStrip,
        subscribers.size(),
        subscribersSection,
        terminalSection,
        statsChartPartial(m),
        latencyChartPartial(m),
        dataExplorerModalPartial(m)
    );

    String script = """
        <script type="application/javascript">
          queueName = "%s";
          dataPageUrl = "rqueue/api/v1/queue-data";
          var chartParams = {"queue": queueName, "type": "STATS", 'aggregationType': 'DAILY'};
          var latencyChartParams = {"queue": queueName, "type": "LATENCY", 'aggregationType': 'DAILY'};
          $(document).ready(function () {
            var chartsRendered = false;
            function renderChartsOnce() {
              if (chartsRendered) return;
              chartsRendered = true;
              drawChart(chartParams, "stats_chart");
              drawChart(latencyChartParams, "latency_chart");
              $('#refresh-chart').click(function () { refreshStatsChart(chartParams, "stats_chart"); });
              $('#refresh-latency-chart').click(function () { refreshLatencyChart(latencyChartParams, "latency_chart"); });
              attachChartEventListeners();
            }
            $('#queue-detail-charts').on('toggle', function () { if (this.open) renderChartsOnce(); });
            $('#explore-queue').on('shown.bs.modal', function () {
              $('#explorer-title').empty().append("Queue:").append("<b>&nbsp;" + queueName + "</b>").append("&nbsp;[" + (dataTypeLabel || dataType) + "]").append("<b>&nbsp;" + dataName + "</b>");
              refreshPage();
            });
          });
        </script>
        """.formatted(jsStr(get(m, "queueName")));
    return base(m, main, script);
  }

  public String renderRunning(Map<String, Object> m) {
    List<Object> header = get(m, "header");
    List<List<Object>> tasks = get(m, "tasks");

    StringBuilder headerRow = new StringBuilder();
    if (header != null) {
      for (Object h : header) {
        headerRow.append("<th>").append(esc(h)).append("</th>");
      }
    }
    StringBuilder taskRows = new StringBuilder();
    if (tasks != null) {
      for (List<Object> task : tasks) {
        taskRows.append("<tr>");
        for (Object td : task) {
          taskRows.append("<td>").append(esc(td)).append("</td>");
        }
        taskRows.append("</tr>");
      }
    }

    String main = """
        <div class="container">
          <div class="row table-responsive">
            <table class="table table-bordered">
              <thead><tr>%s</tr></thead>
              <tbody>%s</tbody>
            </table>
          </div>
        </div>
        """.formatted(headerRow, taskRows);
    return base(m, main, "");
  }

  public String renderUtility(Map<String, Object> m) {
    List<DataType> supportedDataType = get(m, "supportedDataType");
    StringBuilder typeOptions = new StringBuilder();
    if (supportedDataType != null) {
      for (DataType type : supportedDataType) {
        typeOptions.append("<option value=\"").append(esc(type.name())).append("\">")
            .append(esc(type.getDescription())).append("</option>");
      }
    }

    String main = """
        <div class="container">
          <div class="row">
            <div class="col-md-6">
              <div class="col-md-12"><h2>Explore Data</h2></div>
              <div class="explore-data-form col-md-12">
                <div class="form-group">
                  <label for="data-name">Name: <b id="data-name-type"></b></label>
                  <input class="form-control" id="data-name" name="data-name" placeholder="__rq::queue::{job-queue}" type="text">
                </div>
                <div class="form-group display-none" id="data-key-form">
                  <label for="data-key">Key:</label>
                  <input class="form-control" id="data-key" name="data-key" placeholder="any key" type="text">
                </div>
                <div class="clearfix">
                  <button class="btn btn-primary" data-target="#explore-queue" data-toggle="modal" id="view-data" type="button">View</button>
                </div>
              </div>
            </div>
            <div class="col-md-6">
              <div class="col-md-12"><h2>Move Messages</h2></div>
              <div class="message-move-form col-md-12">
                <div class="form-group">
                  <label for="src-data">Source Data Set: <b id="src-data-type"></b></label>
                  <input class="form-control" id="src-data" name="src-data" placeholder="__rq::queue::{job-queue}" type="text">
                </div>
                <div class="form-group">
                  <label for="dst-data">Destination Data Set:<b id="dst-data-type"></b></label>
                  <input class="form-control" id="dst-data" name="dst-data" placeholder="__rq::queue::{job-morgue}" type="text">
                </div>
                <div class="form-group display-none" id="dst-data-type-input-form">
                  <label for="dst-data-type-input">Destination Data Type</label>
                  <select class="form-control" id="dst-data-type-input">
                    <option selected="selected" value="">Select Type</option>
                    %s
                  </select>
                </div>
                <div class="form-group">
                  <label for="number-of-messages">Number of messages: </label>
                  <input class="form-control" id="number-of-messages" name="dst-data" placeholder="100" type="text">
                </div>
                <div class="form-group display-none" id="priority-controller-form">
                  <hr/>
                  <div class="form-group">
                    <label for="priority-type">Priority Type</label>
                    <select class="form-control" id="priority-type" name="priority-type">
                      <option value="">Select</option>
                      <option value="ABS">Absolute</option>
                      <option value="REL">Relative</option>
                    </select>
                  </div>
                  <div class="form-group">
                    <label for="priority-val">Priority</label>
                    <input class="form-control" id="priority-val" name="priority-val" type="number">
                  </div>
                </div>
                <div class="clearfix">
                  <button class="btn btn-danger" id="move-button" type="button">Move</button>
                </div>
              </div>
            </div>
          </div>
        </div>
        %s
        """.formatted(typeOptions, dataExplorerModalPartial(m));

    String script = """
        <script type="application/javascript">
          dataPageUrl = "rqueue/api/v1/view-data";
          var dataKeyEl = $('#data-key');
          var dataNameEl = $('#data-name');
          var srcDataEl = $('#src-data');
          var dstDataEl = $('#dst-data');
          var dstDataTypeInputEl = $('#dst-data-type-input');
          dataNameEl.on("change", function () { updateDataType(this, enableKeyForm); });
          dataKeyEl.on("change", function () { $('#view-data').data('key', $(this).val()); });
          dstDataTypeInputEl.on('change', function () {
            var type = convertVal($(this).val());
            if (type !== 'ZSET') { $('#priority-controller-form').hide(); } else { $('#priority-controller-form').show(); }
          });
          srcDataEl.on('change', function () { disableForms(); updateDataType(this, enableFormsIfRequired); });
          dstDataEl.on('change', function () { disableForms(); updateDataType(this, enableFormsIfRequired); });
          $('#explore-queue').on('shown.bs.modal', function () {
            $('#explorer-title').empty().append("Key:").append("<b>&nbsp;" + dataNameEl.val() + "</b>");
            refreshPage();
          });
        </script>
        """;
    return base(m, main, script);
  }
}
