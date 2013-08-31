import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.base.Charsets;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ByteBuf;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos;
import org.apache.drill.exec.proto.UserBitShared.FieldMetadata;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.vector.*;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.types.TypeProtos.*;
import org.apache.drill.common.types.Types;

import com.sun.codemodel.JType;
import com.sun.codemodel.JCodeModel;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;






