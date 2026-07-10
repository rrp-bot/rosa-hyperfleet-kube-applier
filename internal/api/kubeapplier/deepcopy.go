package kubeapplier

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// -- DynamoDBMetadata --

func (in *DynamoDBMetadata) DeepCopyInto(out *DynamoDBMetadata) {
	*out = *in
}

func (in *DynamoDBMetadata) DeepCopy() *DynamoDBMetadata {
	if in == nil {
		return nil
	}
	out := new(DynamoDBMetadata)
	in.DeepCopyInto(out)
	return out
}

// -- ResourceReference --

func (in *ResourceReference) DeepCopyInto(out *ResourceReference) {
	*out = *in
}

func (in *ResourceReference) DeepCopy() *ResourceReference {
	if in == nil {
		return nil
	}
	out := new(ResourceReference)
	in.DeepCopyInto(out)
	return out
}

// -- ApplyDesire --

func (in *ApplyDesire) DeepCopyInto(out *ApplyDesire) {
	*out = *in
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *ApplyDesire) DeepCopy() *ApplyDesire {
	if in == nil {
		return nil
	}
	out := new(ApplyDesire)
	in.DeepCopyInto(out)
	return out
}

func (in *ApplyDesire) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *ApplyDesireSpec) DeepCopyInto(out *ApplyDesireSpec) {
	*out = *in
	if in.ServerSideApply != nil {
		in, out := &in.ServerSideApply, &out.ServerSideApply
		*out = new(ServerSideApplyConfig)
		(*in).DeepCopyInto(*out)
	}
}

func (in *ApplyDesireSpec) DeepCopy() *ApplyDesireSpec {
	if in == nil {
		return nil
	}
	out := new(ApplyDesireSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *ServerSideApplyConfig) DeepCopyInto(out *ServerSideApplyConfig) {
	*out = *in
	if in.KubeContent != nil {
		in, out := &in.KubeContent, &out.KubeContent
		*out = new(runtime.RawExtension)
		(*in).DeepCopyInto(*out)
	}
}

func (in *ServerSideApplyConfig) DeepCopy() *ServerSideApplyConfig {
	if in == nil {
		return nil
	}
	out := new(ServerSideApplyConfig)
	in.DeepCopyInto(out)
	return out
}

func (in *ApplyDesireStatus) DeepCopyInto(out *ApplyDesireStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]v1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *ApplyDesireStatus) DeepCopy() *ApplyDesireStatus {
	if in == nil {
		return nil
	}
	out := new(ApplyDesireStatus)
	in.DeepCopyInto(out)
	return out
}

// -- ApplyDesireList --

func (in *ApplyDesireList) DeepCopyInto(out *ApplyDesireList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ApplyDesire, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *ApplyDesireList) DeepCopy() *ApplyDesireList {
	if in == nil {
		return nil
	}
	out := new(ApplyDesireList)
	in.DeepCopyInto(out)
	return out
}

func (in *ApplyDesireList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// -- ReadDesire --

func (in *ReadDesire) DeepCopyInto(out *ReadDesire) {
	*out = *in
	in.Status.DeepCopyInto(&out.Status)
}

func (in *ReadDesire) DeepCopy() *ReadDesire {
	if in == nil {
		return nil
	}
	out := new(ReadDesire)
	in.DeepCopyInto(out)
	return out
}

func (in *ReadDesire) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *ReadDesireSpec) DeepCopyInto(out *ReadDesireSpec) {
	*out = *in
}

func (in *ReadDesireSpec) DeepCopy() *ReadDesireSpec {
	if in == nil {
		return nil
	}
	out := new(ReadDesireSpec)
	in.DeepCopyInto(out)
	return out
}

func (in *ReadDesireStatus) DeepCopyInto(out *ReadDesireStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]v1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.KubeContent != nil {
		in, out := &in.KubeContent, &out.KubeContent
		*out = new(runtime.RawExtension)
		(*in).DeepCopyInto(*out)
	}
}

func (in *ReadDesireStatus) DeepCopy() *ReadDesireStatus {
	if in == nil {
		return nil
	}
	out := new(ReadDesireStatus)
	in.DeepCopyInto(out)
	return out
}

// -- ReadDesireList --

func (in *ReadDesireList) DeepCopyInto(out *ReadDesireList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]ReadDesire, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *ReadDesireList) DeepCopy() *ReadDesireList {
	if in == nil {
		return nil
	}
	out := new(ReadDesireList)
	in.DeepCopyInto(out)
	return out
}

func (in *ReadDesireList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
