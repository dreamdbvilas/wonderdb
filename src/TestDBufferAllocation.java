import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class TestDBufferAllocation {
	public static void main(String[] args) {
//		int i = 0;
		List<ByteBuffer> list = new ArrayList<ByteBuffer>();
		try {
			while(true) {
				list.add(ByteBuffer.allocateDirect(2048));
//				i++;
			}
		} catch(Throwable t) {
			System.out.println("allocated: " + list.size());
			t.printStackTrace();
		}
	}
}
