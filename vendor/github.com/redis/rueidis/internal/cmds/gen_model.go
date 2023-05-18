// Code generated DO NOT EDIT

package cmds

import "strconv"

type AiModeldel Completed

func (b Builder) AiModeldel() (c AiModeldel) {
	c = AiModeldel{cs: get(), ks: b.ks}
	c.cs.s = append(c.cs.s, "AI.MODELDEL")
	return c
}

func (c AiModeldel) Key(key string) AiModeldelKey {
	if c.ks&NoSlot == NoSlot {
		c.ks = NoSlot | slot(key)
	} else {
		c.ks = check(c.ks, slot(key))
	}
	c.cs.s = append(c.cs.s, key)
	return (AiModeldelKey)(c)
}

type AiModeldelKey Completed

func (c AiModeldelKey) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelget Completed

func (b Builder) AiModelget() (c AiModelget) {
	c = AiModelget{cs: get(), ks: b.ks, cf: readonly}
	c.cs.s = append(c.cs.s, "AI.MODELGET")
	return c
}

func (c AiModelget) Key(key string) AiModelgetKey {
	if c.ks&NoSlot == NoSlot {
		c.ks = NoSlot | slot(key)
	} else {
		c.ks = check(c.ks, slot(key))
	}
	c.cs.s = append(c.cs.s, key)
	return (AiModelgetKey)(c)
}

type AiModelgetBlob Completed

func (c AiModelgetBlob) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

func (c AiModelgetBlob) Cache() Cacheable {
	c.cs.Build()
	return Cacheable(c)
}

type AiModelgetKey Completed

func (c AiModelgetKey) Meta() AiModelgetMeta {
	c.cs.s = append(c.cs.s, "META")
	return (AiModelgetMeta)(c)
}

func (c AiModelgetKey) Blob() AiModelgetBlob {
	c.cs.s = append(c.cs.s, "BLOB")
	return (AiModelgetBlob)(c)
}

func (c AiModelgetKey) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

func (c AiModelgetKey) Cache() Cacheable {
	c.cs.Build()
	return Cacheable(c)
}

type AiModelgetMeta Completed

func (c AiModelgetMeta) Blob() AiModelgetBlob {
	c.cs.s = append(c.cs.s, "BLOB")
	return (AiModelgetBlob)(c)
}

func (c AiModelgetMeta) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

func (c AiModelgetMeta) Cache() Cacheable {
	c.cs.Build()
	return Cacheable(c)
}

type AiModelstore Completed

func (b Builder) AiModelstore() (c AiModelstore) {
	c = AiModelstore{cs: get(), ks: b.ks}
	c.cs.s = append(c.cs.s, "AI.MODELSTORE")
	return c
}

func (c AiModelstore) Key(key string) AiModelstoreKey {
	if c.ks&NoSlot == NoSlot {
		c.ks = NoSlot | slot(key)
	} else {
		c.ks = check(c.ks, slot(key))
	}
	c.cs.s = append(c.cs.s, key)
	return (AiModelstoreKey)(c)
}

type AiModelstoreBackendOnnx Completed

func (c AiModelstoreBackendOnnx) Cpu() AiModelstoreDeviceCpu {
	c.cs.s = append(c.cs.s, "CPU")
	return (AiModelstoreDeviceCpu)(c)
}

func (c AiModelstoreBackendOnnx) Gpu() AiModelstoreDeviceGpu {
	c.cs.s = append(c.cs.s, "GPU")
	return (AiModelstoreDeviceGpu)(c)
}

type AiModelstoreBackendTf Completed

func (c AiModelstoreBackendTf) Cpu() AiModelstoreDeviceCpu {
	c.cs.s = append(c.cs.s, "CPU")
	return (AiModelstoreDeviceCpu)(c)
}

func (c AiModelstoreBackendTf) Gpu() AiModelstoreDeviceGpu {
	c.cs.s = append(c.cs.s, "GPU")
	return (AiModelstoreDeviceGpu)(c)
}

type AiModelstoreBackendTorch Completed

func (c AiModelstoreBackendTorch) Cpu() AiModelstoreDeviceCpu {
	c.cs.s = append(c.cs.s, "CPU")
	return (AiModelstoreDeviceCpu)(c)
}

func (c AiModelstoreBackendTorch) Gpu() AiModelstoreDeviceGpu {
	c.cs.s = append(c.cs.s, "GPU")
	return (AiModelstoreDeviceGpu)(c)
}

type AiModelstoreBatchsize Completed

func (c AiModelstoreBatchsize) Minbatchsize(minbatchsize int64) AiModelstoreMinbatchsize {
	c.cs.s = append(c.cs.s, "MINBATCHSIZE", strconv.FormatInt(minbatchsize, 10))
	return (AiModelstoreMinbatchsize)(c)
}

func (c AiModelstoreBatchsize) Minbatchtimeout(minbatchtimeout int64) AiModelstoreMinbatchtimeout {
	c.cs.s = append(c.cs.s, "MINBATCHTIMEOUT", strconv.FormatInt(minbatchtimeout, 10))
	return (AiModelstoreMinbatchtimeout)(c)
}

func (c AiModelstoreBatchsize) Inputs(inputCount int64) AiModelstoreInputsInputs {
	c.cs.s = append(c.cs.s, "INPUTS", strconv.FormatInt(inputCount, 10))
	return (AiModelstoreInputsInputs)(c)
}

func (c AiModelstoreBatchsize) Outputs(outputCount int64) AiModelstoreOutputsOutputs {
	c.cs.s = append(c.cs.s, "OUTPUTS", strconv.FormatInt(outputCount, 10))
	return (AiModelstoreOutputsOutputs)(c)
}

func (c AiModelstoreBatchsize) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreBatchsize) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreBlob Completed

func (c AiModelstoreBlob) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreDeviceCpu Completed

func (c AiModelstoreDeviceCpu) Tag(tag string) AiModelstoreTag {
	c.cs.s = append(c.cs.s, "TAG", tag)
	return (AiModelstoreTag)(c)
}

func (c AiModelstoreDeviceCpu) Batchsize(batchsize int64) AiModelstoreBatchsize {
	c.cs.s = append(c.cs.s, "BATCHSIZE", strconv.FormatInt(batchsize, 10))
	return (AiModelstoreBatchsize)(c)
}

func (c AiModelstoreDeviceCpu) Minbatchsize(minbatchsize int64) AiModelstoreMinbatchsize {
	c.cs.s = append(c.cs.s, "MINBATCHSIZE", strconv.FormatInt(minbatchsize, 10))
	return (AiModelstoreMinbatchsize)(c)
}

func (c AiModelstoreDeviceCpu) Minbatchtimeout(minbatchtimeout int64) AiModelstoreMinbatchtimeout {
	c.cs.s = append(c.cs.s, "MINBATCHTIMEOUT", strconv.FormatInt(minbatchtimeout, 10))
	return (AiModelstoreMinbatchtimeout)(c)
}

func (c AiModelstoreDeviceCpu) Inputs(inputCount int64) AiModelstoreInputsInputs {
	c.cs.s = append(c.cs.s, "INPUTS", strconv.FormatInt(inputCount, 10))
	return (AiModelstoreInputsInputs)(c)
}

func (c AiModelstoreDeviceCpu) Outputs(outputCount int64) AiModelstoreOutputsOutputs {
	c.cs.s = append(c.cs.s, "OUTPUTS", strconv.FormatInt(outputCount, 10))
	return (AiModelstoreOutputsOutputs)(c)
}

func (c AiModelstoreDeviceCpu) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreDeviceCpu) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreDeviceGpu Completed

func (c AiModelstoreDeviceGpu) Tag(tag string) AiModelstoreTag {
	c.cs.s = append(c.cs.s, "TAG", tag)
	return (AiModelstoreTag)(c)
}

func (c AiModelstoreDeviceGpu) Batchsize(batchsize int64) AiModelstoreBatchsize {
	c.cs.s = append(c.cs.s, "BATCHSIZE", strconv.FormatInt(batchsize, 10))
	return (AiModelstoreBatchsize)(c)
}

func (c AiModelstoreDeviceGpu) Minbatchsize(minbatchsize int64) AiModelstoreMinbatchsize {
	c.cs.s = append(c.cs.s, "MINBATCHSIZE", strconv.FormatInt(minbatchsize, 10))
	return (AiModelstoreMinbatchsize)(c)
}

func (c AiModelstoreDeviceGpu) Minbatchtimeout(minbatchtimeout int64) AiModelstoreMinbatchtimeout {
	c.cs.s = append(c.cs.s, "MINBATCHTIMEOUT", strconv.FormatInt(minbatchtimeout, 10))
	return (AiModelstoreMinbatchtimeout)(c)
}

func (c AiModelstoreDeviceGpu) Inputs(inputCount int64) AiModelstoreInputsInputs {
	c.cs.s = append(c.cs.s, "INPUTS", strconv.FormatInt(inputCount, 10))
	return (AiModelstoreInputsInputs)(c)
}

func (c AiModelstoreDeviceGpu) Outputs(outputCount int64) AiModelstoreOutputsOutputs {
	c.cs.s = append(c.cs.s, "OUTPUTS", strconv.FormatInt(outputCount, 10))
	return (AiModelstoreOutputsOutputs)(c)
}

func (c AiModelstoreDeviceGpu) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreDeviceGpu) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreInputsInput Completed

func (c AiModelstoreInputsInput) Input(input ...string) AiModelstoreInputsInput {
	c.cs.s = append(c.cs.s, input...)
	return c
}

func (c AiModelstoreInputsInput) Outputs(outputCount int64) AiModelstoreOutputsOutputs {
	c.cs.s = append(c.cs.s, "OUTPUTS", strconv.FormatInt(outputCount, 10))
	return (AiModelstoreOutputsOutputs)(c)
}

func (c AiModelstoreInputsInput) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreInputsInput) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreInputsInputs Completed

func (c AiModelstoreInputsInputs) Input(input ...string) AiModelstoreInputsInput {
	c.cs.s = append(c.cs.s, input...)
	return (AiModelstoreInputsInput)(c)
}

type AiModelstoreKey Completed

func (c AiModelstoreKey) Tf() AiModelstoreBackendTf {
	c.cs.s = append(c.cs.s, "TF")
	return (AiModelstoreBackendTf)(c)
}

func (c AiModelstoreKey) Torch() AiModelstoreBackendTorch {
	c.cs.s = append(c.cs.s, "TORCH")
	return (AiModelstoreBackendTorch)(c)
}

func (c AiModelstoreKey) Onnx() AiModelstoreBackendOnnx {
	c.cs.s = append(c.cs.s, "ONNX")
	return (AiModelstoreBackendOnnx)(c)
}

type AiModelstoreMinbatchsize Completed

func (c AiModelstoreMinbatchsize) Minbatchtimeout(minbatchtimeout int64) AiModelstoreMinbatchtimeout {
	c.cs.s = append(c.cs.s, "MINBATCHTIMEOUT", strconv.FormatInt(minbatchtimeout, 10))
	return (AiModelstoreMinbatchtimeout)(c)
}

func (c AiModelstoreMinbatchsize) Inputs(inputCount int64) AiModelstoreInputsInputs {
	c.cs.s = append(c.cs.s, "INPUTS", strconv.FormatInt(inputCount, 10))
	return (AiModelstoreInputsInputs)(c)
}

func (c AiModelstoreMinbatchsize) Outputs(outputCount int64) AiModelstoreOutputsOutputs {
	c.cs.s = append(c.cs.s, "OUTPUTS", strconv.FormatInt(outputCount, 10))
	return (AiModelstoreOutputsOutputs)(c)
}

func (c AiModelstoreMinbatchsize) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreMinbatchsize) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreMinbatchtimeout Completed

func (c AiModelstoreMinbatchtimeout) Inputs(inputCount int64) AiModelstoreInputsInputs {
	c.cs.s = append(c.cs.s, "INPUTS", strconv.FormatInt(inputCount, 10))
	return (AiModelstoreInputsInputs)(c)
}

func (c AiModelstoreMinbatchtimeout) Outputs(outputCount int64) AiModelstoreOutputsOutputs {
	c.cs.s = append(c.cs.s, "OUTPUTS", strconv.FormatInt(outputCount, 10))
	return (AiModelstoreOutputsOutputs)(c)
}

func (c AiModelstoreMinbatchtimeout) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreMinbatchtimeout) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreOutputsOutput Completed

func (c AiModelstoreOutputsOutput) Output(output ...string) AiModelstoreOutputsOutput {
	c.cs.s = append(c.cs.s, output...)
	return c
}

func (c AiModelstoreOutputsOutput) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreOutputsOutput) Build() Completed {
	c.cs.Build()
	return Completed(c)
}

type AiModelstoreOutputsOutputs Completed

func (c AiModelstoreOutputsOutputs) Output(output ...string) AiModelstoreOutputsOutput {
	c.cs.s = append(c.cs.s, output...)
	return (AiModelstoreOutputsOutput)(c)
}

type AiModelstoreTag Completed

func (c AiModelstoreTag) Batchsize(batchsize int64) AiModelstoreBatchsize {
	c.cs.s = append(c.cs.s, "BATCHSIZE", strconv.FormatInt(batchsize, 10))
	return (AiModelstoreBatchsize)(c)
}

func (c AiModelstoreTag) Minbatchsize(minbatchsize int64) AiModelstoreMinbatchsize {
	c.cs.s = append(c.cs.s, "MINBATCHSIZE", strconv.FormatInt(minbatchsize, 10))
	return (AiModelstoreMinbatchsize)(c)
}

func (c AiModelstoreTag) Minbatchtimeout(minbatchtimeout int64) AiModelstoreMinbatchtimeout {
	c.cs.s = append(c.cs.s, "MINBATCHTIMEOUT", strconv.FormatInt(minbatchtimeout, 10))
	return (AiModelstoreMinbatchtimeout)(c)
}

func (c AiModelstoreTag) Inputs(inputCount int64) AiModelstoreInputsInputs {
	c.cs.s = append(c.cs.s, "INPUTS", strconv.FormatInt(inputCount, 10))
	return (AiModelstoreInputsInputs)(c)
}

func (c AiModelstoreTag) Outputs(outputCount int64) AiModelstoreOutputsOutputs {
	c.cs.s = append(c.cs.s, "OUTPUTS", strconv.FormatInt(outputCount, 10))
	return (AiModelstoreOutputsOutputs)(c)
}

func (c AiModelstoreTag) Blob(blob string) AiModelstoreBlob {
	c.cs.s = append(c.cs.s, "BLOB", blob)
	return (AiModelstoreBlob)(c)
}

func (c AiModelstoreTag) Build() Completed {
	c.cs.Build()
	return Completed(c)
}
