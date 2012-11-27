package org.apache.drill.common.logical.data;

import java.util.Collection;
import java.util.List;

import org.apache.drill.common.expression.visitors.OpVisitor;
import org.apache.drill.common.logical.ValidationError;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

@JsonIdentityInfo(generator=ObjectIdGenerators.IntSequenceGenerator.class, property="@id")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="op")
@JsonInclude(Include.NON_DEFAULT)
public interface LogicalOperator extends Iterable<LogicalOperator>{
	
	public static final Class<?>[] SUB_TYPES = {Sequence.class, Combine.class, Explode.class, Filter.class, Group.class, Join.class, Nest.class, Order.class, Project.class, Scan.class, Transform.class, Union.class};
	
	public void accept(OpVisitor visitor);
	public void registerAsSubscriber(LogicalOperator operator);
	public void setupAndValidate(List<LogicalOperator> operators, Collection<ValidationError> errors);
	@JsonIgnore public int getNestLevel();
}
