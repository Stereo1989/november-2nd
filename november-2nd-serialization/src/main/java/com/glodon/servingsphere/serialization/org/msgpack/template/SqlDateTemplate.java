package com.glodon.servingsphere.serialization.org.msgpack.template;

import com.glodon.servingsphere.serialization.org.msgpack.MessageTypeException;
import com.glodon.servingsphere.serialization.org.msgpack.packer.Packer;
import com.glodon.servingsphere.serialization.org.msgpack.unpacker.Unpacker;

import java.io.IOException;
import java.sql.Date;

/**
 * Created by stereo on 16-8-5.
 */
public class SqlDateTemplate extends AbstractTemplate<Date> {

	@Override
	public void write(Packer pk, Date target, boolean required) throws IOException {
		if (target == null) {
            if (required) {
                throw new MessageTypeException("Attempted to write null");
            }
            pk.writeNil();
            return;
        }
        pk.write((long) target.getTime());
		
	}

	@Override
	public Date read(Unpacker u, Date to, boolean required) throws IOException {
		if (!required && u.trySkipNil()) {
            return null;
        }
        long temp = u.readLong();
        return new Date(temp);
	}

	private SqlDateTemplate(){}
	
	private static SqlDateTemplate instance = new SqlDateTemplate();
	
	public static SqlDateTemplate getInstance(){
		return instance;
	}
}
