import json
import re
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
        print(f"Loading multilingual embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self.model = self.model.to(self.device)
        print(f"Model loaded on {self.device}")

    def load_book_data(self, filepath: str) -> List[Dict]:
        """Load JSONL file containing book pages."""
        data = []
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                data.append(json.loads(line))
        return sorted(data, key=lambda x: x['pos'])

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

        print("Searching for translation pairs...")
        for i in range(len(book_data) - 1):
            page1 = book_data[i]
            page2 = book_data[i + 1]

            is_translation, score = self.is_translation_pair(page1['text'], page2['text'])
            if is_translation:
                translation_pairs.append((page1['pos'], page2['pos'], score))
                print(f"Found translation pair: pages {page1['pos']} → {page2['pos']} (score: {score:.3f})")

        return translation_pairs

    def align_translation_pair(self, text1: str, text2: str) -> Dict:
        """Align a pair of translated texts."""
        # Detect languages
        lang1 = self.detect_language(text1)
        lang2 = self.detect_language(text2)

        print(f"Aligning texts: {lang1} → {lang2}")

        # Extract sentences
        sentences1 = self.extract_sentences(text1)
        sentences2 = self.extract_sentences(text2)

        print(f"Extracted sentences: {len(sentences1)} → {len(sentences2)}")

        # Align sentences
        alignments = self.align_sentences_with_embeddings(sentences1, sentences2)

        return {
            'language1': lang1,
            'language2': lang2,
            'sentences1': sentences1,
            'sentences2': sentences2,
            'alignments': alignments,
            'coverage1': len([a for a in alignments]) / len(sentences1) if sentences1 else 0,
            'coverage2': len([a for a in alignments]) / len(sentences2) if sentences2 else 0
        }

    def process_book(self, filepath: str) -> Dict:
        """Process an entire book to find and align translations."""
        # Load data
        book_data = self.load_book_data(filepath)

        # Find translation pairs
        translation_pairs = self.find_translation_pairs(book_data)

        # Align each pair
        results = {
            'book_id': book_data[0]['book'] if book_data else None,
            'total_pages': len(book_data),
            'translation_pairs': [],
            'alignments': {}
        }

        for pos1, pos2, pair_score in translation_pairs:
            # Get the actual text
            text1 = next(p['text'] for p in book_data if p['pos'] == pos1)
            text2 = next(p['text'] for p in book_data if p['pos'] == pos2)

            # Align the texts
            alignment = self.align_translation_pair(text1, text2)
            alignment['pair_score'] = pair_score

            results['translation_pairs'].append((pos1, pos2))
            results['alignments'][f"{pos1}-{pos2}"] = alignment

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

        alignment = results['alignments'][f"{pos1}-{pos2}"]
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

def main():
    # Initialize aligner with a multilingual model
    # LaBSE is particularly good for sentence alignment across many languages
    aligner = TranslationAligner(model_name='sentence-transformers/LaBSE')

    # Process the book
    results = aligner.process_book('test.jsonl')

    # Visualize results
    visualize_alignments(results)

    # Save results to file
    output_file = 'translation_alignments.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        # Convert results to a serializable format
        serializable_results = {
            'book_id': results['book_id'],
            'total_pages': results['total_pages'],
            'translation_pairs': results['translation_pairs'],
            'alignments': {
                k: {
                    'language1': v['language1'],
                    'language2': v['language2'],
                    'pair_score': v.get('pair_score', 0),
                    'num_sentences1': len(v['sentences1']),
                    'num_sentences2': len(v['sentences2']),
                    'alignments': [
                        {
                            'source_idx': a[0],
                            'target_idx': a[1],
                            'score': float(a[2]),
                            'source_text': v['sentences1'][a[0]],
                            'target_text': v['sentences2'][a[1]]
                        }
                        for a in v['alignments']
                    ]
                }
                for k, v in results['alignments'].items()
            }
        }
        json.dump(serializable_results, f, ensure_ascii=False, indent=2)

    print(f"\n\nResults saved to {output_file}")

if __name__ == "__main__":
    main()
