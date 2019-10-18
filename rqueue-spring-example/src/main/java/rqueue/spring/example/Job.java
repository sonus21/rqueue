package rqueue.spring.example;

public class Job {
  private String id;
  private String message;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public String toString() {
    return "Job[id=" + id + " , message=" + message + "]";
  }
}
