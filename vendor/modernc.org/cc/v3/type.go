// Copyright 2019 The CC Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Parts of the documentation are modified versions originating in the Go
// project, particularly the reflect package, license of which is reproduced
// below.
// ----------------------------------------------------------------------------
// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the GO-LICENSE file.

package cc // import "modernc.org/cc/v3"

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"modernc.org/mathutil"
)

var (
	_ Field = (*field)(nil)

	_ Type = (*aliasType)(nil)
	_ Type = (*arrayType)(nil)
	_ Type = (*attributedType)(nil)
	_ Type = (*bitFieldType)(nil)
	_ Type = (*functionType)(nil)
	_ Type = (*pointerType)(nil)
	_ Type = (*structType)(nil)
	_ Type = (*taggedType)(nil)
	_ Type = (*typeBase)(nil)
	_ Type = (*vectorType)(nil)
	_ Type = noType

	idImag        = dict.sid("imag")
	idReal        = dict.sid("real")
	idVectorSize  = dict.sid("vector_size")
	idVectorSize2 = dict.sid("__vector_size__")

	noType = &typeBase{}

	_ typeDescriptor = (*DeclarationSpecifiers)(nil)
	_ typeDescriptor = (*SpecifierQualifierList)(nil)
	_ typeDescriptor = (*TypeQualifiers)(nil)
	_ typeDescriptor = noTypeDescriptor

	noTypeDescriptor = &DeclarationSpecifiers{}

	// [0]6.3.1.1-1
	//
	// Every integer type has an integer conversion rank defined as
	// follows:
	intConvRank = [maxKind]int{ // Keep Bool first and sorted by rank.
		Bool:      1,
		Char:      2,
		SChar:     2,
		UChar:     2,
		Short:     3,
		UShort:    3,
		Int:       4,
		UInt:      4,
		Long:      5,
		ULong:     5,
		LongLong:  6,
		ULongLong: 6,
	}

	complexIntegerTypes = [maxKind]bool{
		ComplexChar:      true,
		ComplexInt:       true,
		ComplexLong:      true,
		ComplexLongLong:  true,
		ComplexShort:     true,
		ComplexUInt:      true,
		ComplexULong:     true,
		ComplexULongLong: true,
		ComplexUShort:    true,
	}

	complexTypes = [maxKind]bool{
		ComplexChar:       true,
		ComplexDouble:     true,
		ComplexFloat:      true,
		ComplexInt:        true,
		ComplexLong:       true,
		ComplexLongDouble: true,
		ComplexLongLong:   true,
		ComplexShort:      true,
		ComplexUInt:       true,
		ComplexULong:      true,
		ComplexULongLong:  true,
		ComplexUShort:     true,
	}

	integerTypes = [maxKind]bool{
		Bool:      true,
		Char:      true,
		Enum:      true,
		Int:       true,
		Long:      true,
		LongLong:  true,
		SChar:     true,
		Short:     true,
		UChar:     true,
		UInt:      true,
		ULong:     true,
		ULongLong: true,
		UShort:    true,
		Int8:      true,
		Int16:     true,
		Int32:     true,
		Int64:     true,
		Int128:    true,
		UInt8:     true,
		UInt16:    true,
		UInt32:    true,
		UInt64:    true,
		UInt128:   true,
	}

	arithmeticTypes = [maxKind]bool{
		Bool:              true,
		Char:              true,
		ComplexChar:       true,
		ComplexDouble:     true,
		ComplexFloat:      true,
		ComplexInt:        true,
		ComplexLong:       true,
		ComplexLongDouble: true,
		ComplexLongLong:   true,
		ComplexShort:      true,
		ComplexUInt:       true,
		ComplexUShort:     true,
		Double:            true,
		Enum:              true,
		Float:             true,
		Int:               true,
		Long:              true,
		LongDouble:        true,
		LongLong:          true,
		SChar:             true,
		Short:             true,
		UChar:             true,
		UInt:              true,
		ULong:             true,
		ULongLong:         true,
		UShort:            true,
		Int8:              true,
		Int16:             true,
		Int32:             true,
		Int64:             true,
		Int128:            true,
		UInt8:             true,
		UInt16:            true,
		UInt32:            true,
		UInt64:            true,
		UInt128:           true,
	}

	realTypes = [maxKind]bool{
		Bool:              true,
		Char:              true,
		ComplexDouble:     true,
		ComplexFloat:      true,
		ComplexLongDouble: true,
		Double:            true,
		Enum:              true,
		Float:             true,
		Int:               true,
		Long:              true,
		LongDouble:        true,
		LongLong:          true,
		SChar:             true,
		Short:             true,
		UChar:             true,
		UInt:              true,
		ULong:             true,
		ULongLong:         true,
		UShort:            true,
	}
)

type noStorageClass struct{}

func (noStorageClass) auto() bool        { return false }
func (noStorageClass) extern() bool      { return false }
func (noStorageClass) register() bool    { return false }
func (noStorageClass) static() bool      { return false }
func (noStorageClass) threadLocal() bool { return false }
func (noStorageClass) typedef() bool     { return false }

// InvalidType creates a new invalid type.
func InvalidType() Type {
	return noType
}

// Type is the representation of a C type.
//
// Not all methods apply to all kinds of types. Restrictions, if any, are noted
// in the documentation for each method. Use the Kind method to find out the
// kind of type before calling kind-specific methods. Calling a method
// inappropriate to the kind of type causes a run-time panic.
//
// Calling a method on a type of kind Invalid yields an undefined result, but
// does not panic.
type Type interface {
	//TODO bits()

	// Alias returns the type this type aliases. Non typedef types return
	// themselves.
	Alias() Type

	// Align returns the alignment in bytes of a value of this type when
	// allocated in memory.
	Align() int

	// Attributes returns type's attributes, if any.
	Attributes() []*AttributeSpecifier

	// Decay returns itself for non array types and the pointer to array
	// element otherwise.
	Decay() Type

	// Elem returns a type's element type. It panics if the type's Kind is
	// valid but not Array or Ptr.
	Elem() Type

	// EnumType returns the undelying integer type of an enumerated type.  It
	// panics if the type's Kind is valid but not Enum.
	EnumType() Type

	// BitField returns the associated Field of a type. It panics if the
	// type IsBitFieldType returns false.
	BitField() Field

	// FieldAlign returns the alignment in bytes of a value of this type
	// when used as a field in a struct.
	FieldAlign() int

	// FieldByIndex returns the nested field corresponding to the index
	// sequence. It is equivalent to calling Field successively for each
	// index i.  It panics if the type's Kind is valid but not Struct or
	// any complex kind.
	FieldByIndex(index []int) Field

	// FieldByName returns the struct field with the given name and a
	// boolean indicating if the field was found.
	FieldByName(name StringID) (Field, bool)

	// IsIncomplete reports whether type is incomplete.
	IsIncomplete() bool

	// IsComplexIntegerType report whether a type is an integer complex
	// type.
	IsComplexIntegerType() bool

	// IsComplexType report whether a type is a complex type.
	IsComplexType() bool

	// IsArithmeticType report whether a type is an arithmetic type.
	IsArithmeticType() bool

	// IsBitFieldType report whether a type is for a bit field.
	IsBitFieldType() bool

	// IsIntegerType report whether a type is an integer type.
	IsIntegerType() bool

	// IsRealType report whether a type is a real type.
	IsRealType() bool

	// IsScalarType report whether a type is a scalar type.
	IsScalarType() bool

	// IsAliasType returns whether a type is an alias name of another type
	// For eample
	//
	//	typedef int foo;
	//	foo x;	// The type of x reports true from IsAliasType().
	IsAliasType() bool

	// AliasDeclarator returns the typedef declarator of the alias type. It panics
	// if the type is not an alias type.
	AliasDeclarator() *Declarator

	// IsTaggedType returns whether a type is a tagged reference of a enum,
	// struct or union type. For example
	//
	//	struct s { int x; } y;	//  The type of y reports false from IsTaggedType.
	//	struct s z;		//  The type of z reports true from IsTaggedType.
	IsTaggedType() bool

	// IsVariadic reports whether a function type is variadic. It panics if
	// the type's Kind is valid but not Function.
	IsVariadic() bool

	// IsVLA reports whether array is a variable length array. It panics if
	// the type's Kind is valid but not Array.
	IsVLA() bool

	// Kind returns the specific kind of this type.
	Kind() Kind

	// Len returns an array type's length.  It panics if the type's Kind is
	// valid but not Array.
	Len() uintptr

	// LenExpr returns an array type's length expression.  It panics if the
	// type's Kind is valid but not Array or the array is not a VLA.
	LenExpr() *AssignmentExpression

	// NumField returns a struct type's field count.  It panics if the
	// type's Kind is valid but not Struct or any complex kind.
	NumField() int

	// Parameters returns the parameters of a function type. It panics if
	// the type's Kind is valid but not Function.
	Parameters() []*Parameter

	// Real returns the real field of a type. It panics if the type's Kind
	// is valid but not a complex kind.
	Real() Field

	// Imag returns the imaginary field of a type. It panics if the type's
	// Kind is valid but not a complex kind.
	Imag() Field

	// Result returns the result type of a function type. It panics if the
	// type's Kind is valid but not Function.
	Result() Type

	// Size returns the number of bytes needed to store a value of the
	// given type. It panics if type is valid but incomplete.
	Size() uintptr

	// String implements fmt.Stringer.
	String() string

	// Tag returns the tag, of a tagged type or of a struct or union type.
	// Tag panics if the type is not tagged type or a struct or union type.
	Tag() StringID

	// Name returns type name, if any.
	Name() StringID

	// atomic reports whether type has type qualifier "_Atomic".
	atomic() bool

	// isCompatible reports whether a type is compatible with another type.
	// See [0], 6.2.7.
	isCompatible(Type) bool

	// hasConst reports whether type has type qualifier "const".
	hasConst() bool

	// inline reports whether type has function specifier "inline".
	inline() bool

	IsSignedType() bool

	// noReturn reports whether type has function specifier "_NoReturn".
	noReturn() bool

	// restrict reports whether type has type qualifier "restrict".
	restrict() bool

	setLen(uintptr)
	setKind(Kind)

	string(*strings.Builder)

	base() typeBase

	underlyingType() Type

	// IsVolatile reports whether type has type qualifier "volatile".
	IsVolatile() bool
}

// A Field describes a single field in a struct/union.
type Field interface {
	BitFieldBlockFirst() Field
	BitFieldBlockWidth() int
	BitFieldOffset() int
	BitFieldWidth() int
	Declarator() *StructDeclarator
	IsBitField() bool
	IsFlexible() bool // https://en.wikipedia.org/wiki/Flexible_array_member
	Mask() uint64
	Name() StringID  // Can be zero.
	Offset() uintptr // In bytes from the beginning of the struct/union.
	Padding() int
	Promote() Type
	Type() Type // Field type.
}

// A Kind represents the specific kind of type that a Type represents. The zero Kind is not a valid kind.
type Kind uint

const (
	maxTypeSpecifiers = 4 // eg. long long unsigned int
)

var (
	validTypeSpecifiers = map[[maxTypeSpecifiers]TypeSpecifierCase]byte{

		// [2], 6.7.2 Type specifiers, 2.

		//TODO atomic-type-specifier
		{TypeSpecifierBool}:                         byte(Bool),
		{TypeSpecifierChar, TypeSpecifierSigned}:    byte(SChar),
		{TypeSpecifierChar, TypeSpecifierUnsigned}:  byte(UChar),
		{TypeSpecifierChar}:                         byte(Char),
		{TypeSpecifierDouble, TypeSpecifierComplex}: byte(ComplexDouble),
		{TypeSpecifierDouble}:                       byte(Double),
		{TypeSpecifierEnum}:                         byte(Enum),
		{TypeSpecifierFloat, TypeSpecifierComplex}:  byte(ComplexFloat),
		{TypeSpecifierFloat}:                        byte(Float),
		{TypeSpecifierInt, TypeSpecifierLong, TypeSpecifierLong, TypeSpecifierSigned}:   byte(LongLong),
		{TypeSpecifierInt, TypeSpecifierLong, TypeSpecifierLong, TypeSpecifierUnsigned}: byte(ULongLong),
		{TypeSpecifierInt, TypeSpecifierLong, TypeSpecifierLong}:                        byte(LongLong),
		{TypeSpecifierInt, TypeSpecifierLong, TypeSpecifierSigned}:                      byte(Long),
		{TypeSpecifierInt, TypeSpecifierLong, TypeSpecifierUnsigned}:                    byte(ULong),
		{TypeSpecifierInt, TypeSpecifierLong}:                                           byte(Long),
		{TypeSpecifierInt, TypeSpecifierSigned}:                                         byte(Int),
		{TypeSpecifierInt, TypeSpecifierUnsigned}:                                       byte(UInt),
		{TypeSpecifierInt}: byte(Int),
		{TypeSpecifierLong, TypeSpecifierDouble, TypeSpecifierComplex}: byte(ComplexLongDouble),
		{TypeSpecifierLong, TypeSpecifierDouble}:                       byte(LongDouble),
		{TypeSpecifierLong, TypeSpecifierLong, TypeSpecifierSigned}:    byte(LongLong),
		{TypeSpecifierLong, TypeSpecifierLong, TypeSpecifierUnsigned}:  byte(ULongLong),
		{TypeSpecifierLong, TypeSpecifierLong}:                         byte(LongLong),
		{TypeSpecifierLong, TypeSpecifierSigned}:                       byte(Long),
		{TypeSpecifierLong, TypeSpecifierUnsigned}:                     byte(ULong),
		{TypeSpecifierLong}: byte(Long),
		{TypeSpecifierShort, TypeSpecifierInt, TypeSpecifierSigned}:   byte(Short),
		{TypeSpecifierShort, TypeSpecifierInt, TypeSpecifierUnsigned}: byte(UShort),
		{TypeSpecifierShort, TypeSpecifierInt}:                        byte(Short),
		{TypeSpecifierShort, TypeSpecifierSigned}:                     byte(Short),
		{TypeSpecifierShort, TypeSpecifierUnsigned}:                   byte(UShort),
		{TypeSpecifierShort}:                                          byte(Short),
		{TypeSpecifierSigned}:                                         byte(Int),
		{TypeSpecifierStructOrUnion}:                                  byte(Struct),
		{TypeSpecifierTypedefName}:                                    byte(TypedefName), //TODO
		{TypeSpecifierUnsigned}:                                       byte(UInt),
		{TypeSpecifierVoid}:                                           byte(Void),

		// GCC Extensions.

		{TypeSpecifierChar, TypeSpecifierComplex}:                         byte(ComplexChar),
		{TypeSpecifierComplex}:                                            byte(ComplexDouble),
		{TypeSpecifierDecimal128}:                                         byte(Decimal128),
		{TypeSpecifierDecimal32}:                                          byte(Decimal32),
		{TypeSpecifierDecimal64}:                                          byte(Decimal64),
		{TypeSpecifierFloat128}:                                           byte(Float128),
		{TypeSpecifierFloat32x}:                                           byte(Float32x),
		{TypeSpecifierFloat32}:                                            byte(Float32),
		{TypeSpecifierFloat64x}:                                           byte(Float64x),
		{TypeSpecifierFloat64}:                                            byte(Float64),
		{TypeSpecifierInt, TypeSpecifierComplex}:                          byte(ComplexInt),
		{TypeSpecifierInt, TypeSpecifierLong, TypeSpecifierComplex}:       byte(ComplexLong),
		{TypeSpecifierInt8, TypeSpecifierSigned}:                          byte(Int8),
		{TypeSpecifierInt16, TypeSpecifierSigned}:                         byte(Int16),
		{TypeSpecifierInt32, TypeSpecifierSigned}:                         byte(Int32),
		{TypeSpecifierInt64, TypeSpecifierSigned}:                         byte(Int64),
		{TypeSpecifierInt128, TypeSpecifierSigned}:                        byte(Int128),
		{TypeSpecifierInt8, TypeSpecifierUnsigned}:                        byte(UInt8),
		{TypeSpecifierInt16, TypeSpecifierUnsigned}:                       byte(UInt16),
		{TypeSpecifierInt32, TypeSpecifierUnsigned}:                       byte(UInt32),
		{TypeSpecifierInt64, TypeSpecifierUnsigned}:                       byte(UInt64),
		{TypeSpecifierInt128, TypeSpecifierUnsigned}:                      byte(UInt128),
		{TypeSpecifierInt8}:                                               byte(Int8),
		{TypeSpecifierInt16}:                                              byte(Int16),
		{TypeSpecifierInt32}:                                              byte(Int32),
		{TypeSpecifierInt64}:                                              byte(Int64),
		{TypeSpecifierInt128}:                                             byte(Int128),
		{TypeSpecifierLong, TypeSpecifierComplex}:                         byte(ComplexLong),
		{TypeSpecifierLong, TypeSpecifierDouble, TypeSpecifierFloat64x}:   byte(LongDouble),
		{TypeSpecifierLong, TypeSpecifierLong, TypeSpecifierComplex}:      byte(ComplexLongLong),
		{TypeSpecifierShort, TypeSpecifierComplex}:                        byte(ComplexUShort),
		{TypeSpecifierShort, TypeSpecifierUnsigned, TypeSpecifierComplex}: byte(ComplexShort),
		{TypeSpecifierTypeofExpr}:                                         byte(typeofExpr), //TODO
		{TypeSpecifierTypeofType}:                                         byte(typeofType), //TODO
		{TypeSpecifierUnsigned, TypeSpecifierComplex}:                     byte(ComplexUInt),
	}
)

type typeDescriptor interface {
	Node
	auto() bool
	extern() bool
	register() bool
	static() bool
	threadLocal() bool
	typedef() bool
}

type storageClass byte

const (
	fAuto storageClass = 1 << iota
	fExtern
	fRegister
	fStatic
	fThreadLocal
	fTypedef
)

type flag uint8

const (
	// function specifier
	fInline flag = 1 << iota //TODO should go elsewhere
	fNoReturn

	// type qualifier
	fAtomic
	fConst
	fRestrict
	fVolatile

	// other
	fIncomplete
	fSigned // Valid only for integer types.
)

type typeBase struct {
	size uintptr

	flags flag

	align      byte
	fieldAlign byte
	kind       byte
}

func (t *typeBase) check(ctx *context, td typeDescriptor, defaultInt bool) Type {
	k0 := t.kind
	var alignmentSpecifiers []*AlignmentSpecifier
	var attributeSpecifiers []*AttributeSpecifier
	var typeSpecifiers []*TypeSpecifier
	switch n := td.(type) {
	case *DeclarationSpecifiers:
		for ; n != nil; n = n.DeclarationSpecifiers {
			switch n.Case {
			case DeclarationSpecifiersStorage: // StorageClassSpecifier DeclarationSpecifiers
				// nop
			case DeclarationSpecifiersTypeSpec: // TypeSpecifier DeclarationSpecifiers
				typeSpecifiers = append(typeSpecifiers, n.TypeSpecifier)
			case DeclarationSpecifiersTypeQual: // TypeQualifier DeclarationSpecifiers
				// nop
			case DeclarationSpecifiersFunc: // FunctionSpecifier DeclarationSpecifiers
				// nop
			case DeclarationSpecifiersAlignSpec: // AlignmentSpecifier DeclarationSpecifiers
				alignmentSpecifiers = append(alignmentSpecifiers, n.AlignmentSpecifier)
			case DeclarationSpecifiersAttribute: // AttributeSpecifier DeclarationSpecifiers
				attributeSpecifiers = append(attributeSpecifiers, n.AttributeSpecifier)
			default:
				panic(internalError())
			}
		}
	case *SpecifierQualifierList:
		for ; n != nil; n = n.SpecifierQualifierList {
			switch n.Case {
			case SpecifierQualifierListTypeSpec: // TypeSpecifier SpecifierQualifierList
				typeSpecifiers = append(typeSpecifiers, n.TypeSpecifier)
			case SpecifierQualifierListTypeQual: // TypeQualifier SpecifierQualifierList
				// nop
			case SpecifierQualifierListAlignSpec: // AlignmentSpecifier SpecifierQualifierList
				alignmentSpecifiers = append(alignmentSpecifiers, n.AlignmentSpecifier)
			case SpecifierQualifierListAttribute: // AttributeSpecifier SpecifierQualifierList
				attributeSpecifiers = append(attributeSpecifiers, n.AttributeSpecifier)
			default:
				panic(internalError())
			}
		}
	case *TypeQualifiers:
		for ; n != nil; n = n.TypeQualifiers {
			if n.Case == TypeQualifiersAttribute {
				attributeSpecifiers = append(attributeSpecifiers, n.AttributeSpecifier)
			}
		}
	default:
		panic(internalError())
	}

	if len(typeSpecifiers) > maxTypeSpecifiers {
		ctx.err(typeSpecifiers[maxTypeSpecifiers].Position(), "too many type specifiers")
		typeSpecifiers = typeSpecifiers[:maxTypeSpecifiers]
	}

	sort.Slice(typeSpecifiers, func(i, j int) bool {
		return typeSpecifiers[i].Case < typeSpecifiers[j].Case
	})
	var k [maxTypeSpecifiers]TypeSpecifierCase
	for i, v := range typeSpecifiers {
		k[i] = v.Case
	}
	switch {
	case len(typeSpecifiers) == 0:
		if !defaultInt {
			break
		}

		k[0] = TypeSpecifierInt
		fallthrough
	default:
		var ok bool
		if t.kind, ok = validTypeSpecifiers[k]; !ok {
			s := k[:]
			for len(s) > 1 && s[len(s)-1] == TypeSpecifierVoid {
				s = s[:len(s)-1]
			}
			ctx.err(td.Position(), "invalid type specifiers combination: %v", s)
			return t
		}

		if t.kind == byte(LongDouble) && ctx.cfg.LongDoubleIsDouble {
			t.kind = byte(Double)
		}
	}
	switch len(alignmentSpecifiers) {
	case 0:
		//TODO set alignment from model
	case 1:
		align := alignmentSpecifiers[0].align()
		if align > math.MaxUint8 {
			panic(internalError())
		}
		t.align = byte(align)
		t.fieldAlign = t.align
	default:
		ctx.err(alignmentSpecifiers[1].Position(), "multiple alignment specifiers")
	}

	abi := ctx.cfg.ABI
	switch k := t.Kind(); k {
	case typeofExpr, typeofType, Struct, Union, Enum:
		// nop
	default:
		if integerTypes[k] && abi.isSignedInteger(k) {
			t.flags |= fSigned
		}
		if v, ok := abi.Types[k]; ok {
			t.size = uintptr(abi.size(k))
			if t.align != 0 {
				break
			}

			t.align = byte(v.Align)
			t.fieldAlign = byte(v.FieldAlign)
			break
		}

		//TODO ctx.err(td.Position(), "missing model item for %s", t.Kind())
	}

	typ := Type(t)
	switch k := t.Kind(); k {
	case TypedefName:
		ts := typeSpecifiers[0]
		tok := ts.Token
		nm := tok.Value
		d := ts.resolvedIn.typedef(nm, tok)
		typ = &aliasType{nm: nm, d: d}
	case Enum:
		typ = typeSpecifiers[0].EnumSpecifier.typ
	case Struct, Union:
		t.kind = k0
		typ = typeSpecifiers[0].StructOrUnionSpecifier.typ
	case typeofExpr, typeofType:
		typ = typeSpecifiers[0].typ
	default:
		if complexTypes[k] {
			typ = ctx.cfg.ABI.Type(k)
		}
	}
	return typ
}

// atomic implements Type.
func (t *typeBase) atomic() bool { return t.flags&fAtomic != 0 }

// Attributes implements Type.
func (t *typeBase) Attributes() (a []*AttributeSpecifier) { return nil }

// Alias implements Type.
func (t *typeBase) Alias() Type { return t }

// IsAliasType implements Type.
func (t *typeBase) IsAliasType() bool { return false }

func (t *typeBase) AliasDeclarator() *Declarator {
	panic(internalErrorf("%s: AliasDeclarator of invalid type", t.Kind()))
}

// IsTaggedType implements Type.
func (t *typeBase) IsTaggedType() bool { return false }

// Align implements Type.
func (t *typeBase) Align() int { return int(t.align) }

// BitField implements Type.
func (t *typeBase) BitField() Field {
	if t.Kind() == Invalid {
		return nil
	}

	panic(internalErrorf("%s: BitField of invalid type", t.Kind()))
}

// base implements Type.
func (t *typeBase) base() typeBase { return *t }

// isCompatible implements Type.
func (t *typeBase) isCompatible(u Type) bool {
	// [0], 6.2.7

	if t.Kind() == Invalid || u.Kind() == Invalid {
		return false
	}

	// Two types have compatible type if their types are the same.
	// Additional rules for determining whether two types are compatible
	// are described in 6.7.2 for type specifiers, in 6.7.3 for type
	// qualifiers, and in 6.7.5 for declarators
	if t == u {
		return true
	}

	switch t.Kind() {
	case Enum:
		return u.IsIntegerType() && t.compatibleQualifiers(u) //TODO enum sizes
	case
		Array,
		Function,
		Ptr,
		Struct,
		TypedefName,
		Union:

		panic(internalErrorf("%s: isCompatible of invalid type", t.Kind()))
	}

	return (t.Kind() == u.Kind() || t.IsIntegerType() && u.Kind() == Enum) && t.compatibleQualifiers(u) //TODO enum sizes
}

func (t *typeBase) compatibleQualifiers(u Type) bool {
	const mask = fAtomic | fConst | fRestrict | fVolatile

	// [0], 6.7.3
	//
	// For two qualified types to be compatible, both shall have the
	// identically qualified version of a compatible type; the order of
	// type qualifiers within a list of specifiers or qualifiers does not
	// affect the specified type.
	return t.flags&mask == u.base().flags&mask
}

// Decay implements Type.
func (t *typeBase) Decay() Type {
	if t.Kind() != Array {
		return t
	}

	panic(internalErrorf("%s: Decay of invalid type", t.Kind()))
}

// Elem implements Type.
func (t *typeBase) Elem() Type {
	if t.Kind() == Invalid {
		return t
	}

	panic(internalErrorf("%s: Elem of invalid type", t.Kind()))
}

// EnumType implements Type.
func (t *typeBase) EnumType() Type {
	if t.Kind() == Invalid {
		return t
	}

	panic(internalErrorf("%s: EnumType of invalid type", t.Kind()))
}

// hasConst implements Type.
func (t *typeBase) hasConst() bool { return t.flags&fConst != 0 }

// FieldAlign implements Type.
func (t *typeBase) FieldAlign() int { return int(t.fieldAlign) }

// FieldByIndex implements Type.
func (t *typeBase) FieldByIndex([]int) Field {
	if t.Kind() == Invalid {
		return nil
	}

	panic(internalErrorf("%s: FieldByIndex of invalid type", t.Kind()))
}

// NumField implements Type.
func (t *typeBase) NumField() int {
	if t.Kind() == Invalid {
		return 0
	}

	panic(internalErrorf("%s: NumField of invalid type", t.Kind()))
}

// FieldByName implements Type.
func (t *typeBase) FieldByName(StringID) (Field, bool) {
	if t.Kind() == Invalid {
		return nil, false
	}

	panic(internalErrorf("%s: FieldByName of invalid type", t.Kind()))
}

// IsIncomplete implements Type.
func (t *typeBase) IsIncomplete() bool { return t.flags&fIncomplete != 0 }

// inline implements Type.
func (t *typeBase) inline() bool { return t.flags&fInline != 0 }

// IsIntegerType implements Type.
func (t *typeBase) IsIntegerType() bool { return integerTypes[t.kind] }

// IsArithmeticType implements Type.
func (t *typeBase) IsArithmeticType() bool { return arithmeticTypes[t.Kind()] }

// IsComplexType implements Type.
func (t *typeBase) IsComplexType() bool { return complexTypes[t.Kind()] }

// IsComplexIntegerType implements Type.
func (t *typeBase) IsComplexIntegerType() bool { return complexIntegerTypes[t.Kind()] }

// IsBitFieldType implements Type.
func (t *typeBase) IsBitFieldType() bool { return false }

// IsRealType implements Type.
func (t *typeBase) IsRealType() bool { return realTypes[t.Kind()] }

// IsScalarType implements Type.
func (t *typeBase) IsScalarType() bool { return t.IsArithmeticType() || t.Kind() == Ptr }

// IsSignedType implements Type.
func (t *typeBase) IsSignedType() bool {
	if !integerTypes[t.kind] {
		panic(internalErrorf("%s: IsSignedType of non-integer type", t.Kind()))
	}

	return t.flags&fSigned != 0
}

// IsVariadic implements Type.
func (t *typeBase) IsVariadic() bool {
	if t.Kind() == Invalid {
		return false
	}

	panic(internalErrorf("%s: IsVariadic of invalid type", t.Kind()))
}

// IsVLA implements Type.
func (t *typeBase) IsVLA() bool {
	if t.Kind() == Invalid {
		return false
	}

	panic(internalErrorf("%s: IsVLA of invalid type", t.Kind()))
}

// Kind implements Type.
func (t *typeBase) Kind() Kind { return Kind(t.kind) }

// Len implements Type.
func (t *typeBase) Len() uintptr { panic(internalErrorf("%s: Len of non-array type", t.Kind())) }

// LenExpr implements Type.
func (t *typeBase) LenExpr() *AssignmentExpression {
	panic(internalErrorf("%s: LenExpr of non-array type", t.Kind()))
}

// noReturn implements Type.
func (t *typeBase) noReturn() bool { return t.flags&fNoReturn != 0 }

// restrict implements Type.
func (t *typeBase) restrict() bool { return t.flags&fRestrict != 0 }

// Parameters implements Type.
func (t *typeBase) Parameters() []*Parameter {
	if t.Kind() == Invalid {
		return nil
	}

	panic(internalErrorf("%s: Parameters of invalid type", t.Kind()))
}

// Result implements Type.
func (t *typeBase) Result() Type {
	if t.Kind() == Invalid {
		return noType
	}

	panic(internalErrorf("%s: Result of invalid type", t.Kind()))
}

// Real implements Type
func (t *typeBase) Real() Field {
	if t.Kind() == Invalid {
		return nil
	}

	panic(internalErrorf("%s: Real of invalid type", t.Kind()))
}

// Imag implements Type
func (t *typeBase) Imag() Field {
	if t.Kind() == Invalid {
		return nil
	}

	panic(internalErrorf("%s: Imag of invalid type", t.Kind()))
}

// Size implements Type.
func (t *typeBase) Size() uintptr {
	if t.IsIncomplete() {
		panic(internalError())
	}

	return t.size
}

// setLen implements Type.
func (t *typeBase) setLen(uintptr) {
	if t.Kind() == Invalid {
		return
	}

	panic(internalErrorf("%s: setLen of non-array type", t.Kind()))
}

// setKind implements Type.
func (t *typeBase) setKind(k Kind) { t.kind = byte(k) }

// underlyingType implements Type.
func (t *typeBase) underlyingType() Type { return t }

// IsVolatile implements Type.
func (t *typeBase) IsVolatile() bool { return t.flags&fVolatile != 0 }

// String implements Type.
func (t *typeBase) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// Name implements Type.
func (t *typeBase) Name() StringID { return 0 }

// Tag implements Type.
func (t *typeBase) Tag() StringID {
	panic(internalErrorf("%s: Tag of invalid type", t.Kind()))
}

// string implements Type.
func (t *typeBase) string(b *strings.Builder) {
	spc := ""
	if t.atomic() {
		b.WriteString("atomic")
		spc = " "
	}
	if t.hasConst() {
		b.WriteString(spc)
		b.WriteString("const")
		spc = " "
	}
	if t.inline() {
		b.WriteString(spc)
		b.WriteString("inline")
		spc = " "
	}
	if t.noReturn() {
		b.WriteString(spc)
		b.WriteString("_NoReturn")
		spc = " "
	}
	if t.restrict() {
		b.WriteString(spc)
		b.WriteString("restrict")
		spc = " "
	}
	if t.IsVolatile() {
		b.WriteString(spc)
		b.WriteString("volatile")
		spc = " "
	}
	b.WriteString(spc)
	switch k := t.Kind(); k {
	case Enum:
		b.WriteString("enum")
	case Invalid:
		// nop
	case Struct:
		b.WriteString("struct")
	case Union:
		b.WriteString("union")
	case Ptr:
		b.WriteString("pointer")
	case typeofExpr, typeofType:
		panic(internalError())
	default:
		b.WriteString(k.String())
	}
}

type attributedType struct {
	Type
	attr []*AttributeSpecifier
}

// Alias implements Type.
func (t *attributedType) Alias() Type { return t }

// String implements Type.
func (t *attributedType) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// string implements Type.
func (t *attributedType) string(b *strings.Builder) {
	for _, v := range t.attr {
		panic(v.Position())
	}
	t.Type.string(b)
}

// Attributes implements Type.
func (t *attributedType) Attributes() []*AttributeSpecifier { return t.attr }

type pointerType struct {
	typeBase

	elem           Type
	typeQualifiers Type
}

// Alias implements Type.
func (t *pointerType) Alias() Type { return t }

// Attributes implements Type.
func (t *pointerType) Attributes() (a []*AttributeSpecifier) { return t.elem.Attributes() }

// isCompatible implements Type.
func (t *pointerType) isCompatible(u Type) bool {
	return u.underlyingType().Kind() == Ptr && t.Elem().underlyingType().isCompatible(u.Elem().underlyingType())
}

// Decay implements Type.
func (t *pointerType) Decay() Type { return t }

// Elem implements Type.
func (t *pointerType) Elem() Type { return t.elem }

// underlyingType implements Type.
func (t *pointerType) underlyingType() Type { return t }

// String implements Type.
func (t *pointerType) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// string implements Type.
func (t *pointerType) string(b *strings.Builder) {
	if t := t.typeQualifiers; t != nil {
		t.string(b)
	}
	b.WriteString("pointer to ")
	t.Elem().string(b)
}

type arrayType struct {
	typeBase

	expr   *AssignmentExpression
	decay  Type
	elem   Type
	length uintptr

	vla bool
}

// Alias implements Type.
func (t *arrayType) Alias() Type { return t }

// IsVLA implements Type.
func (t *arrayType) IsVLA() bool { return t.vla || t.elem.Kind() == Array && t.Elem().IsVLA() }

// isCompatible implements Type.
func (t *arrayType) isCompatible(u Type) bool {
	panic("TODO")
}

// String implements Type.
func (t *arrayType) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// string implements Type.
func (t *arrayType) string(b *strings.Builder) {
	b.WriteString("array of ")
	if t.Len() != 0 {
		fmt.Fprintf(b, "%d ", t.Len())
	}
	t.Elem().string(b)
}

// Attributes implements Type.
func (t *arrayType) Attributes() (a []*AttributeSpecifier) { return t.elem.Attributes() }

// Decay implements Type.
func (t *arrayType) Decay() Type { return t.decay }

// Elem implements Type.
func (t *arrayType) Elem() Type { return t.elem }

// Len implements Type.
func (t *arrayType) Len() uintptr { return t.length }

// LenExpr implements Type.
func (t *arrayType) LenExpr() *AssignmentExpression {
	if !t.vla {
		panic(internalErrorf("%s: LenExpr of non variable length array", t.Kind()))
	}

	return t.expr
}

// setLen implements Type.
func (t *arrayType) setLen(n uintptr) {
	t.typeBase.flags &^= fIncomplete
	t.length = n
	if t.Elem() != nil {
		t.size = t.length * t.Elem().Size()
	}
}

// underlyingType implements Type.
func (t *arrayType) underlyingType() Type { return t }

type aliasType struct {
	nm StringID
	d  *Declarator
}

// IsAliasType implements Type.
func (t *aliasType) IsAliasType() bool { return true }

func (t *aliasType) AliasDeclarator() *Declarator { return t.d }

// IsTaggedType implements Type.
func (t *aliasType) IsTaggedType() bool { return false }

// Alias implements Type.
func (t *aliasType) Alias() Type { return t.d.Type() }

// Align implements Type.
func (t *aliasType) Align() int { return t.d.Type().Align() }

// Attributes implements Type.
func (t *aliasType) Attributes() (a []*AttributeSpecifier) { return nil }

// BitField implements Type.
func (t *aliasType) BitField() Field { return t.d.Type().BitField() }

// isCompatible implements Type.
func (t *aliasType) isCompatible(u Type) bool {
	return t == u || t.underlyingType().isCompatible(u.underlyingType())
}

// EnumType implements Type.
func (t *aliasType) EnumType() Type { return t.d.Type().EnumType() }

// Decay implements Type.
func (t *aliasType) Decay() Type { return t.d.Type().Decay() }

// Elem implements Type.
func (t *aliasType) Elem() Type { return t.d.Type().Elem() }

// FieldAlign implements Type.
func (t *aliasType) FieldAlign() int { return t.d.Type().FieldAlign() }

// NumField implements Type.
func (t *aliasType) NumField() int { return t.d.Type().NumField() }

// FieldByIndex implements Type.
func (t *aliasType) FieldByIndex(i []int) Field { return t.d.Type().FieldByIndex(i) }

// FieldByName implements Type.
func (t *aliasType) FieldByName(s StringID) (Field, bool) { return t.d.Type().FieldByName(s) }

// IsIncomplete implements Type.
func (t *aliasType) IsIncomplete() bool { return t.d.Type().IsIncomplete() }

// IsArithmeticType implements Type.
func (t *aliasType) IsArithmeticType() bool { return t.d.Type().IsArithmeticType() }

// IsComplexType implements Type.
func (t *aliasType) IsComplexType() bool { return t.d.Type().IsComplexType() }

// IsComplexIntegerType implements Type.
func (t *aliasType) IsComplexIntegerType() bool { return t.d.Type().IsComplexIntegerType() }

// IsBitFieldType implements Type.
func (t *aliasType) IsBitFieldType() bool { return t.d.Type().IsBitFieldType() }

// IsIntegerType implements Type.
func (t *aliasType) IsIntegerType() bool { return t.d.Type().IsIntegerType() }

// IsRealType implements Type.
func (t *aliasType) IsRealType() bool { return t.d.Type().IsRealType() }

// IsScalarType implements Type.
func (t *aliasType) IsScalarType() bool { return t.d.Type().IsScalarType() }

// IsVLA implements Type.
func (t *aliasType) IsVLA() bool { return t.d.Type().IsVLA() }

// IsVariadic implements Type.
func (t *aliasType) IsVariadic() bool { return t.d.Type().IsVariadic() }

// Kind implements Type.
func (t *aliasType) Kind() Kind { return t.d.Type().Kind() }

// Len implements Type.
func (t *aliasType) Len() uintptr { return t.d.Type().Len() }

// LenExpr implements Type.
func (t *aliasType) LenExpr() *AssignmentExpression { return t.d.Type().LenExpr() }

// Parameters implements Type.
func (t *aliasType) Parameters() []*Parameter { return t.d.Type().Parameters() }

// Result implements Type.
func (t *aliasType) Result() Type { return t.d.Type().Result() }

// Real implements Type
func (t *aliasType) Real() Field { return t.d.Type().Real() }

// Imag implements Type
func (t *aliasType) Imag() Field { return t.d.Type().Imag() }

// Size implements Type.
func (t *aliasType) Size() uintptr { return t.d.Type().Size() }

// String implements Type.
func (t *aliasType) String() string { return t.nm.String() }

// Tag implements Type.
func (t *aliasType) Tag() StringID { return t.d.Type().Tag() }

// Name implements Type.
func (t *aliasType) Name() StringID { return t.nm }

// atomic implements Type.
func (t *aliasType) atomic() bool { return t.d.Type().atomic() }

// base implements Type.
func (t *aliasType) base() typeBase { return t.d.Type().base() }

// hasConst implements Type.
func (t *aliasType) hasConst() bool { return t.d.Type().hasConst() }

// inline implements Type.
func (t *aliasType) inline() bool { return t.d.Type().inline() }

// IsSignedType implements Type.
func (t *aliasType) IsSignedType() bool { return t.d.Type().IsSignedType() }

// noReturn implements Type.
func (t *aliasType) noReturn() bool { return t.d.Type().noReturn() }

// restrict implements Type.
func (t *aliasType) restrict() bool { return t.d.Type().restrict() }

// setLen implements Type.
func (t *aliasType) setLen(n uintptr) { t.d.Type().setLen(n) }

// setKind implements Type.
func (t *aliasType) setKind(k Kind) { t.d.Type().setKind(k) }

// string implements Type.
func (t *aliasType) string(b *strings.Builder) { b.WriteString(t.nm.String()) }

func (t *aliasType) underlyingType() Type { return t.d.Type().underlyingType() }

// IsVolatile implements Type.
func (t *aliasType) IsVolatile() bool { return t.d.Type().IsVolatile() }

type field struct {
	bitFieldMask uint64 // bits: 3, bitOffset: 2 -> 0x1c. Valid only when isBitField is true.
	blockStart   *field // First bit field of the block this bit field belongs to.
	d            *StructDeclarator
	offset       uintptr // In bytes from start of the struct.
	promote      Type
	typ          Type

	name StringID // Can be zero.

	isBitField bool
	isFlexible bool // https://en.wikipedia.org/wiki/Flexible_array_member

	bitFieldOffset byte // In bits from bit 0 within the field. Valid only when isBitField is true.
	bitFieldWidth  byte // Width of the bit field in bits. Valid only when isBitField is true.
	blockWidth     byte // Total width of the bit field block this bit field belongs to.
	pad            byte
}

func (f *field) BitFieldBlockFirst() Field     { return f.blockStart }
func (f *field) BitFieldBlockWidth() int       { return int(f.blockWidth) }
func (f *field) BitFieldOffset() int           { return int(f.bitFieldOffset) }
func (f *field) BitFieldWidth() int            { return int(f.bitFieldWidth) }
func (f *field) Declarator() *StructDeclarator { return f.d }
func (f *field) IsBitField() bool              { return f.isBitField }
func (f *field) IsFlexible() bool              { return f.isFlexible }
func (f *field) Mask() uint64                  { return f.bitFieldMask }
func (f *field) Name() StringID                { return f.name }
func (f *field) Offset() uintptr               { return f.offset }
func (f *field) Padding() int                  { return int(f.pad) } // N/A for bitfields
func (f *field) Promote() Type                 { return f.promote }
func (f *field) Type() Type                    { return f.typ }

func (f *field) string(b *strings.Builder) {
	b.WriteString(f.name.String())
	if f.isBitField {
		fmt.Fprintf(b, ":%d", f.bitFieldWidth)
	}
	b.WriteByte(' ')
	f.typ.string(b)
}

type structType struct { //TODO implement Type
	*typeBase

	attr   []*AttributeSpecifier
	fields []*field
	m      map[StringID]*field

	tag StringID
}

// Alias implements Type.
func (t *structType) Alias() Type { return t }

// Tag implements Type.
func (t *structType) Tag() StringID { return t.tag }

// isCompatible implements Type.
func (t *structType) isCompatible(u Type) bool { return t == u }

func (t *structType) check(ctx *context, n Node) *structType {
	if t == nil {
		return nil
	}

	// Reject ambiguous names.
	for _, f := range t.fields {
		if f.Name() != 0 {
			continue
		}

		switch x := f.Type().(type) {
		case *structType:
			for _, f2 := range x.fields {
				nm := f2.Name()
				if nm == 0 {
					continue
				}

				if _, ok := t.m[nm]; ok {
					ctx.errNode(n, "ambiguous field name %q", nm)
				}
			}
		default:
			//TODO report err
		}
	}

	return ctx.cfg.ABI.layout(ctx, n, t)
}

// Real implements Type
func (t *structType) Real() Field {
	if !complexTypes[t.Kind()] {
		panic(internalErrorf("%s: Real of invalid type", t.Kind()))
	}

	f, ok := t.FieldByName(idReal)
	if !ok {
		panic(internalError())
	}

	return f
}

// Imag implements Type
func (t *structType) Imag() Field {
	if !complexTypes[t.Kind()] {
		panic(internalErrorf("%s: Real of invalid type", t.Kind()))
	}

	f, ok := t.FieldByName(idImag)
	if !ok {
		panic(internalError())
	}

	return f
}

// Decay implements Type.
func (t *structType) Decay() Type { return t }

func (t *structType) underlyingType() Type { return t }

// String implements Type.
func (t *structType) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// Name implements Type.
func (t *structType) Name() StringID { return t.tag }

// string implements Type.
func (t *structType) string(b *strings.Builder) {
	switch {
	case complexTypes[t.Kind()]:
		b.WriteString(t.Kind().String())
		return
	default:
		b.WriteString(t.Kind().String())
	}
	b.WriteByte(' ')
	if t.tag != 0 {
		b.WriteString(t.tag.String())
		b.WriteByte(' ')
	}
	b.WriteByte('{')
	for _, v := range t.fields {
		v.string(b)
		b.WriteString("; ")
	}
	b.WriteByte('}')
}

// FieldByIndex implements Type.
func (t *structType) FieldByIndex(i []int) Field {
	if len(i) > 1 {
		panic("TODO")
	}

	return t.fields[i[0]]
}

// FieldByName implements Type.
func (t *structType) FieldByName(name StringID) (Field, bool) {
	best := mathutil.MaxInt
	return t.fieldByName(name, 0, &best, 0)
}

func (t *structType) fieldByName(name StringID, lvl int, best *int, off uintptr) (Field, bool) {
	if lvl >= *best {
		return nil, false
	}

	if f, ok := t.m[name]; ok {
		*best = lvl
		if off != 0 {
			g := *f
			g.offset += off
			f = &g
		}
		return f, ok
	}

	for _, f := range t.fields {
		if f.Name() != 0 {
			continue
		}

		if f, ok := f.Type().(*structType).fieldByName(name, lvl+1, best, off+f.offset); ok {
			return f, ok
		}
	}

	return nil, false
}

func (t *structType) NumField() int { return len(t.fields) }

type taggedType struct {
	*typeBase
	resolutionScope Scope
	typ             Type

	tag StringID
}

// IsTaggedType implements Type.
func (t *taggedType) IsTaggedType() bool { return true }

// Tag implements Type.
func (t *taggedType) Tag() StringID { return t.tag }

// Alias implements Type.
func (t *taggedType) Alias() Type { return t.underlyingType() }

// isCompatible implements Type.
func (t *taggedType) isCompatible(u Type) bool {
	return t == u || t.Kind() == u.Kind() && t.underlyingType().isCompatible(u.underlyingType())
}

// Decay implements Type.
func (t *taggedType) Decay() Type { return t }

// IsIncomplete implements Type.
func (t *taggedType) IsIncomplete() bool {
	u := t.underlyingType()
	return u == noType || u.IsIncomplete()
}

// String implements Type.
func (t *taggedType) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// Name implements Type.
func (t *taggedType) Name() StringID { return t.tag }

// NumField implements Type.
func (t *taggedType) NumField() int { return t.underlyingType().NumField() }

// FieldByIndex implements Type.
func (t *taggedType) FieldByIndex(i []int) Field { return t.underlyingType().FieldByIndex(i) }

// FieldByName implements Type.
func (t *taggedType) FieldByName(s StringID) (Field, bool) { return t.underlyingType().FieldByName(s) }

// IsSignedType implements Type.
func (t *taggedType) IsSignedType() bool { return t.underlyingType().IsSignedType() }

// EnumType implements Type.
func (t *taggedType) EnumType() Type { return t.underlyingType() }

// string implements Type.
func (t *taggedType) string(b *strings.Builder) {
	t.typeBase.string(b)
	b.WriteByte(' ')
	b.WriteString(t.tag.String())
}

func (t *taggedType) underlyingType() Type {
	if t.typ != nil {
		return t.typ
	}

	k := t.Kind()
	for s := t.resolutionScope; s != nil; s = s.Parent() {
		for _, v := range s[t.tag] {
			switch x := v.(type) {
			case *Declarator, *StructDeclarator:
			case *EnumSpecifier:
				if k == Enum && x.Case == EnumSpecifierDef {
					t.typ = x.Type()
					return t.typ.underlyingType()
				}
			case *StructOrUnionSpecifier:
				if x.typ == nil {
					break
				}

				switch k {
				case Struct:
					if typ := x.Type(); typ.Kind() == Struct {
						t.typ = typ
						return typ.underlyingType()
					}
				case Union:
					if typ := x.Type(); typ.Kind() == Union {
						t.typ = typ
						return typ.underlyingType()
					}
				}
			default:
				panic(internalError())
			}
		}
	}
	t.typ = noType
	return noType
}

// Size implements Type.
func (t *taggedType) Size() (r uintptr) {
	return t.underlyingType().Size()
}

// Align implements Type.
func (t *taggedType) Align() int { return t.underlyingType().Align() }

// FieldAlign implements Type.
func (t *taggedType) FieldAlign() int { return t.underlyingType().FieldAlign() }

type functionType struct {
	typeBase
	params    []*Parameter
	paramList []StringID

	result Type

	variadic bool
}

// Alias implements Type.
func (t *functionType) Alias() Type { return t }

// isCompatible implements Type.
func (t *functionType) isCompatible(u Type) bool {
	panic("TODO")
}

// Decay implements Type.
func (t *functionType) Decay() Type { return t }

// String implements Type.
func (t *functionType) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// string implements Type.
func (t *functionType) string(b *strings.Builder) {
	b.WriteString("function(")
	for i, v := range t.params {
		v.Type().string(b)
		if i < len(t.params)-1 {
			b.WriteString(", ")
		}
	}
	if t.variadic {
		b.WriteString(", ...")
	}
	b.WriteString(")")
	if t.result != nil && t.result.Kind() != Void {
		b.WriteString(" returning ")
		t.result.string(b)
	}
}

// Parameters implements Type.
func (t *functionType) Parameters() []*Parameter { return t.params }

// Result implements Type.
func (t *functionType) Result() Type { return t.result }

// IsVariadic implements Type.
func (t *functionType) IsVariadic() bool { return t.variadic }

type bitFieldType struct {
	Type
	field *field
}

// Alias implements Type.
func (t *bitFieldType) Alias() Type { return t }

// IsBitFieldType implements Type.
func (t *bitFieldType) IsBitFieldType() bool { return true }

// BitField implements Type.
func (t *bitFieldType) BitField() Field { return t.field }

type vectorType struct {
	typeBase

	elem   Type
	length uintptr
}

// Alias implements Type.
func (t *vectorType) Alias() Type { return t }

// IsVLA implements Type.
func (t *vectorType) IsVLA() bool { return false }

// isCompatible implements Type.
func (t *vectorType) isCompatible(u Type) bool {
	panic("TODO")
}

// String implements Type.
func (t *vectorType) String() string {
	var b strings.Builder
	t.string(&b)
	return strings.TrimSpace(b.String())
}

// string implements Type.
func (t *vectorType) string(b *strings.Builder) {
	fmt.Fprintf(b, "vector of %d ", t.Len())
	t.Elem().string(b)
}

// Attributes implements Type.
func (t *vectorType) Attributes() (a []*AttributeSpecifier) { return t.elem.Attributes() }

// Elem implements Type.
func (t *vectorType) Elem() Type { return t.elem }

// Len implements Type.
func (t *vectorType) Len() uintptr { return t.length }

// LenExpr implements Type.
func (t *vectorType) LenExpr() *AssignmentExpression {
	panic(internalErrorf("%s: LenExpr of non variable length array", t.Kind()))
}

// setLen implements Type.
func (t *vectorType) setLen(n uintptr) {
	panic("internal error")
}

// underlyingType implements Type.
func (t *vectorType) underlyingType() Type { return t }
