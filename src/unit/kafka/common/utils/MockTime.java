package kafka.common.utils;

public class MockTime implements Time {
  
  private long ms = 0;

  @Override
  public long milliseconds() {
    return ms;
  }

  @Override
  public void sleep(long ms) {
    this.ms = ms;
  }

}
