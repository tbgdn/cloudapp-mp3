
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import javax.rmi.CORBA.Util;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
	private BufferedReader reader;
	private String inputFilePath;

	public FileReaderSpout(String filePath){
		this.inputFilePath = filePath;
	}


  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

	  try {
		  reader = new BufferedReader(new FileReader(this.inputFilePath));
	  } catch (FileNotFoundException e) {
		  e.printStackTrace();
	  }
	  this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

	  String line = null;
	  try {
		  line = reader.readLine();
		  if (line == null){
			  Utils.sleep(2000);
		  }else{
			  _collector.emit(new Values(line));
		  }
	  } catch (IOException e) {
		  e.printStackTrace();
		  Utils.sleep(2000);
	  }


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
	  try {
		  reader.close();
	  } catch (IOException e) {
		  e.printStackTrace();
	  }
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
