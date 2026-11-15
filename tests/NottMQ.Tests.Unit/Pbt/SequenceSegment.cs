using System.Buffers;

namespace NottMQ.Tests.Unit.Pbt;

/// <summary>
/// Linked list node to construct multi-segment <see cref="ReadOnlySequence{Byte}"/> instances for fragmentation testing.
/// </summary>
internal sealed class SequenceSegment<T> : ReadOnlySequenceSegment<T>
{
    public SequenceSegment(ReadOnlyMemory<T> memory)
    {
        Memory = memory;
    }

    public SequenceSegment<T> Append(ReadOnlyMemory<T> memory)
    {
        var nextSegment = new SequenceSegment<T>(memory)
        {
            RunningIndex = RunningIndex + Memory.Length
        };
        Next = nextSegment;
        return nextSegment;
    }

    public static ReadOnlySequence<T> Create(IReadOnlyList<ReadOnlyMemory<T>> chunks)
    {
        if (chunks.Count == 0) return ReadOnlySequence<T>.Empty;
        if (chunks.Count == 1) return new ReadOnlySequence<T>(chunks[0]);

        var first = new SequenceSegment<T>(chunks[0]);
        var last = first;

        for (int i = 1; i < chunks.Count; i++)
        {
            last = last.Append(chunks[i]);
        }

        return new ReadOnlySequence<T>(first, 0, last, last.Memory.Length);
    }
}
