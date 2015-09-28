//package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;


public class stemUDF extends EvalFunc<String>
{
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
 		return null;
		try{
		Porter p = new Porter();
		String str = (String)input.get(0);
		return String.valueOf(p.stripAffixes(str));
		}catch(Exception e){
		throw new IOException("Caught exception processing input row ", e);
		}
    }
}