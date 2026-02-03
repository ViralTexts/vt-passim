import argparse
import json
import re
import sys
from typing import List, Tuple, Dict, Optional
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.tokenize import sent_tokenize
from langdetect import detect_langs
import torch

# Download required NLTK data
nltk.download('punkt', quiet=True)
nltk.download('punkt_tab')

class TranslationAligner:
    def __init__(self, model_name: str = 'sentence-transformers/LaBSE'):
        """
        Initialize with a multilingual sentence embedding model.
        Good options:
        - 'sentence-transformers/LaBSE' (109 languages, excellent for alignment)
        - 'sentence-transformers/paraphrase-multilingual-mpnet-base-v2' (50+ languages)
        - 'sentence-transformers/distiluse-base-multilingual-cased-v2' (50+ languages)
        """
        self.model = SentenceTransformer(model_name)
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self.model = self.model.to(self.device)

    def detect_language(self, text: str) -> str:
        """Detect the primary language of a text."""
        try:
            # Clean text for better language detection
            clean_text = self.clean_text_for_lang_detection(text)
            if len(clean_text.strip()) < 10:
                return 'unknown'

            langs = detect_langs(clean_text)
            return langs[0].lang if langs else 'unknown'
        except:
            # Fallback: check for Greek characters
            if self.has_greek_characters(text):
                return 'el'  # Greek
            return 'unknown'

    def has_greek_characters(self, text: str) -> bool:
        """Check if text contains significant Greek characters."""
        greek_count = sum(1 for char in text if '\u0370' <= char <= '\u03ff' or '\u1f00' <= char <= '\u1fff')
        total_alpha = sum(1 for char in text if char.isalpha())
        return greek_count > total_alpha * 0.3 if total_alpha > 0 else False

    def clean_text_for_lang_detection(self, text: str) -> str:
        """Clean text for language detection."""
        # Remove numbers, excessive punctuation, and extra whitespace
        text = re.sub(r'\d+', '', text)
        text = re.sub(r'[^\w\s]', ' ', text, flags=re.UNICODE)
        text = ' '.join(text.split())
        return text[:1000]  # Use first 1000 chars for detection

    def is_translation_pair(self, text1: str, text2: str, threshold: float = 0.5) -> Tuple[bool, float]:
        """
        Determine if two texts are likely translations of each other using embeddings.
        Returns (is_translation, similarity_score)
        """
        # Check if languages are different
        lang1 = self.detect_language(text1)
        lang2 = self.detect_language(text2)

        if lang1 == lang2 and lang1 != 'unknown':
            return False, 0.0

        # Check length similarity (translations typically have similar lengths)
        len_ratio = len(text1) / len(text2) if len(text2) > 0 else 0
        if len_ratio < 0.3 or len_ratio > 3.0:
            return False, 0.0

        # Get embeddings for both texts
        # For efficiency, we'll use a sample of the text if it's very long
        sample1 = text1[:3000] if len(text1) > 3000 else text1
        sample2 = text2[:3000] if len(text2) > 3000 else text2

        try:
            # Encode texts to get embeddings
            embedding1 = self.model.encode(sample1, convert_to_tensor=True, device=self.device)
            embedding2 = self.model.encode(sample2, convert_to_tensor=True, device=self.device)

            # Calculate cosine similarity
            similarity = torch.cosine_similarity(
                embedding1.unsqueeze(0),
                embedding2.unsqueeze(0)
            ).item()

            return similarity > threshold, similarity
        except Exception as e:
            print(f"Error computing similarity: {e}")
            return False, 0.0

    def extract_sentences(self, text: str) -> List[str]:
        """Extract sentences from text."""
        # Handle Greek-specific punctuation
        if self.has_greek_characters(text):
            # Greek uses ; for question mark and · for semicolon
            text = text.replace(';', '?')
            text = text.replace('·', ';')

        # Use NLTK to split sentences
        sentences = sent_tokenize(text)

        # Clean and filter sentences
        cleaned_sentences = []
        for s in sentences:
            s = s.strip()
            # Filter out very short sentences and page numbers
            if len(s) > 15 and not re.match(r'^\d+\.?\s*$', s):
                cleaned_sentences.append(s)

        return cleaned_sentences

    def align_sentences_with_embeddings(self, source_sentences: List[str],
                                      target_sentences: List[str],
                                      threshold: float = 0.5) -> List[Tuple[int, int, float]]:
        """
        Align sentences using multilingual embeddings.
        Returns list of (source_idx, target_idx, score) tuples.
        """
        if not source_sentences or not target_sentences:
            return []

        try:
            # Encode all sentences at once for efficiency
            source_embeddings = self.model.encode(
                source_sentences,
                convert_to_tensor=True,
                device=self.device,
                show_progress_bar=False
            )
            target_embeddings = self.model.encode(
                target_sentences,
                convert_to_tensor=True,
                device=self.device,
                show_progress_bar=False
            )

            # Calculate similarity matrix
            similarity_matrix = torch.matmul(
                source_embeddings,
                target_embeddings.T
            ).cpu().numpy()

            # Find best alignments using a greedy approach
            alignments = []
            used_targets = set()

            # Sort all possible pairs by similarity
            all_pairs = []
            for i in range(len(source_sentences)):
                for j in range(len(target_sentences)):
                    all_pairs.append((i, j, similarity_matrix[i, j]))

            # Sort by similarity score (descending)
            all_pairs.sort(key=lambda x: x[2], reverse=True)

            # Greedily select best non-conflicting alignments
            used_sources = set()
            for source_idx, target_idx, score in all_pairs:
                if (source_idx not in used_sources and
                    target_idx not in used_targets and
                    score > threshold):
                    alignments.append((source_idx, target_idx, score))
                    used_sources.add(source_idx)
                    used_targets.add(target_idx)

            # Sort by source index for readability
            return sorted(alignments, key=lambda x: x[0])

        except Exception as e:
            print(f"Error in sentence alignment: {e}")
            return []

    def find_translation_pairs(self, book_data: List[Dict]) -> List[Tuple[int, int, float]]:
        """
        Find pairs of pages that are translations of each other.
        Returns list of (page1_pos, page2_pos, similarity_score).
        """
        translation_pairs = []

        for i in range(len(book_data) - 1):
            page1 = book_data[i]
            page2 = book_data[i + 1]

            is_translation, score = self.is_translation_pair(page1['text'], page2['text'])
            if is_translation:
                translation_pairs.append((i, i+1, score))

        return translation_pairs

    def align_translation_pair(self, text1: str, text2: str) -> Dict:
        """Align a pair of translated texts."""
        # Detect languages
        lang1 = self.detect_language(text1)
        lang2 = self.detect_language(text2)

        # Extract sentences
        sentences1 = self.extract_sentences(text1)
        sentences2 = self.extract_sentences(text2)

        # Align sentences
        raw = self.align_sentences_with_embeddings(sentences1, sentences2)

        alignments = [a for a in raw
                      if (self.detect_language(sentences1[a[0]]) == lang1
                          and self.detect_language(sentences2[a[1]]) == lang2)]

        amap = {src: trg for src, trg, score in alignments}
        ascore = {src: float(score) for src, trg, score in alignments}
        expanded = []
        for src in range(len(sentences1)):
            if src in amap:
                trg = amap[src]
            # include single-sentence gaps
            elif ((src-1) in amap and (src+1) in amap
                  and (amap.get(src+1, -1) - amap.get(src-1, -1)) == 2):
                trg = amap[src-1] + 1
            else:
                trg = None
            if trg != None:
                expanded.append({
                    'source_idx': src,
                    'target_idx': trg,
                    'score': ascore.get(src, 0),
                    'source_text': sentences1[src],
                    'target_text': sentences2[trg]
                })

        return {
            'language1': lang1,
            'language2': lang2,
            'sentences1': sentences1,
            'sentences2': sentences2,
            'alignments': expanded,
            'coverage1': len([a for a in alignments]) / len(sentences1) if sentences1 else 0,
            'coverage2': len([a for a in alignments]) / len(sentences2) if sentences2 else 0
        }

    def process_book(self, book_data: List[Dict]) -> Dict:
        """Process an entire book to find and align translations."""
        # Find translation pairs
        translation_pairs = self.find_translation_pairs(book_data)

        # Align each pair
        results = {
            'book_id': book_data[0]['book'] if book_data else None,
            'total_pages': len(book_data),
            'alignments': []
        }

        lang1 = {}
        for pos1, pos2, pair_score in translation_pairs:
            # Get the actual text
            text1 = book_data[pos1]['text']
            text2 = book_data[pos2]['text']

            # Align the texts
            alignment = self.align_translation_pair(text1, text2)
            alignment['pair_score'] = pair_score
            alignment['idx'] = pos1

            if len(alignment['alignments']) > 0:
                results['alignments'].append(alignment)
                L1 = alignment['language1']
                lang1[L1] = lang1.get(L1, 0) + len(alignment['alignments'])

        results['src_lang'] = max(lang1, key=lang1.get)
        # Remove alignments in non-max direction
        # results['alignments'] = [r for r in results['alignments']
        #                          if r['language1'] == results['src_lang']]

        return results

def visualize_alignments(results: Dict, max_pairs: int = 3):
    """Visualize the alignment results."""
    print(f"\n{'='*80}")
    print(f"TRANSLATION ALIGNMENT RESULTS")
    print(f"{'='*80}")
    print(f"Book ID: {results['book_id']}")
    print(f"Total pages: {results['total_pages']}")
    print(f"Found {len(results['translation_pairs'])} translation pairs")

    # Show detailed results for first few pairs
    for i, (pos1, pos2) in enumerate(results['translation_pairs'][:max_pairs]):
        print(f"\n{'-'*80}")
        print(f"Translation pair {i+1}: Page {pos1} → Page {pos2}")

        alignment = results['alignments'][pos1]
        print(f"Languages: {alignment['language1']} → {alignment['language2']}")
        print(f"Page-level similarity: {alignment['pair_score']:.3f}")
        print(f"Sentences: {len(alignment['sentences1'])} → {len(alignment['sentences2'])}")
        print(f"Aligned pairs: {len(alignment['alignments'])}")
        print(f"Coverage: {alignment['coverage1']:.1%} of source, {alignment['coverage2']:.1%} of target")

        # Show aligned sentences
        if alignment['alignments']:
            print(f"\nTop aligned sentences (by similarity score):")
            # Sort by score to show best alignments first
            sorted_alignments = sorted(alignment['alignments'], key=lambda x: x[2], reverse=True)

            for j, (src_idx, tgt_idx, score) in enumerate(sorted_alignments[:5]):
                print(f"\n  [{j+1}] Score: {score:.3f}")
                src_sent = alignment['sentences1'][src_idx]
                tgt_sent = alignment['sentences2'][tgt_idx]

                # Truncate long sentences for display
                if len(src_sent) > 150:
                    src_sent = src_sent[:150] + "..."
                if len(tgt_sent) > 150:
                    tgt_sent = tgt_sent[:150] + "..."

                print(f"  Source: {src_sent}")
                print(f"  Target: {tgt_sent}")

def load_book_data(filepath: str) -> List[Dict]:
    """Load JSONL file containing book pages."""
    data = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return sorted(data, key=lambda x: x['pos'])

def main(args):
    parser = argparse.ArgumentParser(description='Facing page alignment',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('inputPath', metavar='<path>', help='input data')
    parser.add_argument('outputPath', metavar='<path>', help='output')
    config = parser.parse_args(args)

    # Initialize aligner with a multilingual model
    # LaBSE is particularly good for sentence alignment across many languages
    aligner = TranslationAligner(model_name='sentence-transformers/LaBSE')

    # Process the book
    data = load_book_data(config.inputPath)
    results = aligner.process_book(data)

    # Visualize results
    # visualize_alignments(results)

    # Save results to file
    with open(config.outputPath, 'w', encoding='utf-8') as f:
        # Convert results to a serializable format
        for v in results['alignments']:
            serializable_results = {
                'book_id': results['book_id'],
                'total_pages': results['total_pages'],
                'src_lang': results['src_lang'],
                'idx': v['idx'],
                'language1': v['language1'],
                'language2': v['language2'],
                'pair_score': v.get('pair_score', 0),
                'num_sentences1': len(v['sentences1']),
                'num_sentences2': len(v['sentences2']),
                'alignments': v['alignments']
            }
            print(json.dumps(serializable_results, ensure_ascii=False), file=f)
            
if __name__ == "__main__":
    main(sys.argv[1:])
