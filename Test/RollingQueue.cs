using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    public class RollingQueue<T> : Queue<T>
    {
        /// <summary>
        /// Constructs a new rolling queue.
        /// </summary>
        /// <param name="size">The size of the queue to maintain.</param>
        public RollingQueue(int size)
        {
            this.Size = size;
        }

        /// <summary>
        /// Gets or sets the size of the rolling queue.
        /// </summary>
        public int Size
        {
            get;
            set;
        }

        /// <summary>
        /// Enqueues an item to the rolling queue.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        new public void Enqueue(T item)
        {
            // Enqueue a new item
            base.Enqueue(item);

            // Dequeue if there's too many elements
            if (this.Count > this.Size)
                this.Dequeue();
        }

    }
}
