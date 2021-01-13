### Type Inference in Flux
--------------------------

Flux is a strongly and statically typed language.
Flux does not require explicit type annotions but rather infers the most general type of a variable.
Flux supports parametric polymorphism.

#### Rules
----------

In order to perform type inference Flux has a collection of rules that govern the relationship between types and expressions.
Take the following program for example.

```
f = (a, b) => return a + b
x = f(a:1, b:2)
```

The following rules will be used to solve for the types in the above program:

1. Binary Addition Rule

    typeof(a + b) = typeof(a)
    typeof(a + b) = typeof(b)

2. Function Rule

    typeof(f) = [ typeof(a), typeof(b) ] -> typeof(return statement)

3. Call Expression Rule

    typeof(f) = [ typeof(a), typeof(b) ] -> typeof( f(a:1, b:2) )

4. Variable Assigment Rule

    typeof(x) = typeof( f(a:1, b:2) )

#### Algorithm
--------------

The type inference algorithm that Flux uses is based on Wand's algorithm and .
The algorithm operates on a semantic graph.
The algorithm builds off some basic concepts.

1. Monotype and polytypes

    A type can be a monotype or a polytype.
    Monotypes can be only one type, for example `int`, `string` and `boolean` are monotypes.
    Polytypes can be all types, for example function type `{x:t0} -> t0` takes as input an object with a single key `x` that can have any type.
    The return value of the function is the type of the `x` parameter.
    This function's type is a polytype because it is defined for all possible types.
    It is said the `t0` is a free type variable because it is free to be any type.

2. Type expression

    A type expression is description of a type.
    Type expressions may describe monotypes and polytypes.

3. Type annotations

    Every node in a semantic graph may have a type.
    A type annotation records the node's type.
    A node's type may be unknown, in which case its annotation is an instance of a type variable.
    A type variable is a placeholder for an unknown type.

4. Type environment

    A type environment maps identifiers to a type scheme.
    A type scheme is a type expression with a list of free type variables.
    A scheme can be "instantiated" into an equivalent but unique type expression.
    This instantiation process is what enables parametric polymorphism.

5. Constraints

    A constraint is a requirement that a given type expression be equal to another type expression.
    There are two constraint domains, types and kinds. The type domain applies constraints between types, for example that bool must be equal to bool.
    The kind domain applies kind constraints to a specific type, for example an object type must have a "foo" property.
    Using these separate constraint domains enables structural polymorphism.

6. Unification

    To unify two types is to ensure that the two types are equal.
    If the types are not equal then a type error has occurred.
    When a type is unified with a type variable, the variable is "set" to the value of that type.
    By updating the type variables in this manner the solution to the inference problem is determined.
    Unification is applied in both kind and type domains simultaneously, because unify kinds may require that types be unified and vice versa.


The algorithm proceeds in three main phases:

1. Annotation - walk the graph and annotate semantic expressions with type expressions.
2. Generate constraints - using the constraint rules walk the graph and generate a list of constraints.
3. Unification - solve the unification problem presented by the set of constraints.

Once the process is completed the type is known for all nodes or an error has occurred.
Note, nodes may still have polymorphic types.

#### Polymorphic Definitions
--------------

##### Parametric Polymorphism

Parameteric polymorphism means that a type in Flux may represent multiple types.
For example a polymorphic function in flux looks like this:

    add = (a,b) => a + b

Since Flux does not require synactic type annotations, the types of `a` and `b` are free to be whatever type they need.
For example `add` can be called for any type where the `+` operator is defined.

    add(a:1,b:1) // ints
    add(a:1.0,b:1.0) // floats
    add(a:"hello ",b:"world") // strings

This behavior is called parametric polymorphic because the of `a` and `b` can be parameterized via a type variable.
That type variable can take on multiple types throughout the Flux program.


##### Structural Polymorphism

Structural polymorphism is the idea that structures (in the case of Flux objects) can be used in many ways.
This is also somethimes called row polymorphism.
Structural polymorphism enables the following:

    john = {name: "John", last: "Smith", age: 42, height: 71}
    jane = {name: "Jane", age: 24.5, height: 61, weight: 110.5}
    // John and Jane are different objects with different keys.
    // Structural polymorphism allows Flux to treat them as the same type so long
    // as we only use the keys that they have in common that also have the same type.

    // As such we can define a function `name` that can take any record that contains a `name` key.
    name = (person) => person.name

    name(person: john) // John
    name(person: jane) // Jane

    lastName = (person) => person.last

    lastName(person: john) // Smith
    lastName(person: jane) // type error, the object jane does not contain the `last` key.
    // NOTE: The error is a type error, not a runtime error as the error is discovered during type analysis not during execution.

    age = (person) => person.age

    age(person: john) // 42 (int)
    age(person: jane) // 24.5 (float)
    // NOTE: age is polymorphic in both the objects it can consume and in its return value.


Flux also leverages structural polymorphism to implement its default values to functions.
Internally Flux functions always take a single parameter, that parameter is always an object that must contain the non-defaulted keyword parameters of the function.
Here is a trivial example:

    foo = (x,y,z=1) => x + y + z
    do = (f) => f(x:5, y:4)
    // We can pass `foo` as the parameter `f` to do because,
    // it can be called with x and y and z's default will automatically be used.
    do(f:foo)

Here is a more complex but real example using a builtin helper function called `aggregateWindow` that manages aggregations over windows of time.
    
    // AggregateWindow applies an aggregate function to fixed windows of time.
    aggregateWindow = (every, fn, columns=["_value"], timeSrc="_stop",timeDst="_time", table=<-) =>
    	table
    		|> window(every:every)
    		|> fn(columns:columns)
    		|> duplicate(column:timeSrc,as:timeDst)
    		|> window(every:inf, timeCol:timeDst)

    from(bucket:"example")
        |> range(start:-1h)
        // We can pass mean as the fn parameter to aggregateWindow because it takes a columns parameter and accepts a piped table parameter.
        // Similarly we can pass any aggregate function.
        |> aggregateWindow(every:1m, fn: mean)

