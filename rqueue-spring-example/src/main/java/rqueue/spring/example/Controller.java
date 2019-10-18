package rqueue.spring.example;

import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor(onConstructor = @__(@Autowired))
public class Controller {
  private RqueueMessageSender rqueueMessageSender;

  @GetMapping(value = "/push")
  public String getCities(
      String q,
      String msg,
      @RequestParam(required = false) Integer numRetries,
      @RequestParam(required = false) Long delay) {
    if (numRetries == null && delay == null) {
      rqueueMessageSender.put(q, msg);
    } else if (numRetries == null) {
      rqueueMessageSender.put(q, msg, delay);
    } else {
      rqueueMessageSender.put(q, msg, numRetries, delay);
    }
    return "Message sent successfully";
  }

  @GetMapping("job")
  public String sendJobNotification() {
    Job job = new Job();
    job.setId(UUID.randomUUID().toString());
    job.setMessage("Hi this is " + job.getId());
    rqueueMessageSender.put("job-queue", job);
    return job.toString();
  }
}
